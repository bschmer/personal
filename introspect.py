#! /usr/bin/env python

'''
Simple module to extract ELK contents into a format suitable to be
re-imported using git-to-the.  All object ids are adjusted to a human
readable name based upon the title of the object.  The adjusted ids
are updated in all objects that depend upon the object.
'''

import os
import os.path
import sys
import json
import configparser
import argparse
import uuid
import requests
import natsort
import elasticsearch

def normalize(data):
    '''
    Normalize object names into ids that are suitable for both saving to git and
    importing into ELK.
    '''
    data = data.strip().replace(' ', '-')
    while '--' in data:
        data = data.replace('--', '-')
    return "".join([c for c in data if c.isalpha() or c.isdigit() or c == '-'])

class ELK(object):
    '''
    ELK interface class.
    '''
    def __init__(self, elasticsearch_host, user=None, password=None, # pylint: disable=too-many-arguments
                 elasticsearch_port=9200, kibana_host=None,
                 kibana_port=5601, ca_certs=None, directory='elk_backup',
                 **kwds):
        '''
        Initialize the ELK object and get a connection to Elasticsearch
        '''
        if not kibana_host:
            kibana_host = elasticsearch_host
        self._es_url = '%s:%d' % (elasticsearch_host, elasticsearch_port)
        self._kibana_url = '%s:%d' % (kibana_host, kibana_port)

        self._params = kwds
        if user or password:
            self._params['http_auth'] = (user, password)
        if ca_certs:
            self._params.update(dict(use_ssl=True,
                                     verify_certs=True,
                                     ca_certs=ca_certs))
            self._kibana_url = 'https://%s:%d' % (kibana_host, kibana_port)
        else:
            self._kibana_url = 'http://%s:%d' % (kibana_host, kibana_port)

        self._es = elasticsearch.Elasticsearch(self._es_url, **self._params)
        self._spaces = None
        self._directory = directory
        print self._es.info()

    def _get_all(self, index, size=50):
        '''
        Internal method to retrieve and yield all documents in an index, using
        the specified scroll size.
        '''
        page = self._es.search(index=index, # pylint: disable=unexpected-keyword-arg
                               scroll='2m',
                               size=size,
                               body={"size": size, "query": {"match_all": {}}})
        for item in page['hits']['hits']:
            yield item
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
        while scroll_size > 0:
            page = self._es.scroll(scroll_id=sid, scroll='2m') # pylint: disable=unexpected-keyword-arg
            for item in page['hits']['hits']:
                yield item
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])

    def get_all(self, indexname, size=100, limit=50):
        '''
        Retrieve all documents for the specified index, capping the number at
        the value specified in limit.
        '''
        if limit < size:
            size = limit
        for index, item in enumerate(self._get_all(indexname, size=size)):
            yield item
            if index >= limit:
                break


    def get_templates(self):
        '''
        Retrieve all of the index templates.  Return a dictionary containing a
        mapping of template names to template settings.
        '''
        alltemplates = {}
        templates = self._es.cat.templates(format='json') # pylint: disable=unexpected-keyword-arg
        for item in natsort.natsorted(templates, lambda x: x['name']):
            template = self._es.indices.get_template(item['name'])
            alltemplates.update(template)

        return alltemplates

    def get_indices(self):
        '''
        Retrieve all of the index data.  Return a dictionary containing a
        mapping of index name to index information.
        '''
        indices = self._es.cat.indices(format='json') # pylint: disable=unexpected-keyword-arg
        return dict(zip([x['index'] for x in indices], indices))

    def get_ilm_policies(self):
        '''
        Retrieve all ILM policies information.  Return a dictionary containing
        a mapping of policy name to policy settings.
        '''
        ilm_policies = self._es.transport.perform_request('GET', '/_ilm/policy')
        return ilm_policies

    def get_aliases(self):
        '''
        Retrireve all alias information.  Return a dictionary containing a
        mapping of alias name to alias settings.
        '''
        aliases = [x for x in self._es.cat.aliases(format='json') if x['is_write_index'] == 'true']  # pylint: disable=unexpected-keyword-arg
        return_value = {}
        for item in aliases:
            return_value[item['alias']] = {
                'aliases': {
                    item['alias']: {
                        'is_write_index': True
                    }
                },
                '_use_date_math_in_index_name': True
            }
        return return_value

    def backup_es(self):
        '''
        Gather all Elasicsearch data and save it into a format that's usable
        to save into git.
        '''
        data = (self.get_templates, self.get_ilm_policies, self.get_aliases)
        for method in sorted(data):
            for key, value in method().items():
                objtype = method.__name__.replace('get_', '')
                if objtype == 'aliases':
                    objtype = 'index'
                targetdir = '%s/elasticsearch/%s' % (self._directory, objtype)
                if not os.path.exists(targetdir):
                    os.makedirs(targetdir, 0755)
                json.dump(value, open('%s/%s.json' % (targetdir, key), 'w'))

    def _kibana_call(self, url, headers=None, **kwds):
        '''
        Make a call to the kibana api, enriching with the kibana authorization
        information if applicable.
        '''
        kibana_params = kwds
        if 'http_auth' in self._params:
            kibana_params['auth'] = self._params['http_auth']
        url = self._kibana_url + url
        return requests.get(url, headers=headers, **kibana_params)

    def get_spaces(self, force=False):
        '''
        Get a list of spaces.
        '''
        if force or not self._spaces:
            response = self._kibana_call('/api/spaces/space')
            self._spaces = [x['id'] for x in response.json()]
        return self._spaces

    def dump(self, space=None, *obj_types, **kwds): # pylint: disable=keyword-arg-before-vararg
        '''
        Dump all kibana saved objects.
        '''
        if space and space not in self.get_spaces():
            obj_types = [space] + obj_types
            space = 'default'
        print space
        return_value = {}
        if not obj_types:
            obj_types = ('config', 'map', 'canvas-workpad', 'canvas-element', 'lens',
                         'query', 'index-pattern', 'visualization', 'dashboard',
                         'search', 'url', 'timelion-sheet')
        for obj_type in obj_types:
            print obj_type
            page = 1
            while True:
                if space and space != 'default':
                    url = '/s/%s/api/saved_objects/_find' % space
                else:
                    url = '/api/saved_objects/_find'
                print url
                if obj_type:
                    url += '?type={}&page={}&per_page=50'.format(obj_type, page)
                request = self._kibana_call(url, headers={'kbn-xsrf': 'true'}, **kwds)
                response = request.json()
                #{u'per_page': 20, u'total': 7, u'page': 1, u'saved_objects'
                if 'saved_objects' in response:
                    if obj_type not in return_value:
                        return_value[obj_type] = []
                    return_value[obj_type].extend(response.pop('saved_objects'))
                    print response
                    if len(return_value[obj_type]) >= response['total']:
                        break
                else:
                    print request
                page += 1
        return return_value

    def _adjust_ids(self, space):
        '''
        Gather the items in the space and adjust the ids, saving them in a map.
        '''
        dumpdata = self.dump(space)
        idmap = dict()
        for key, value in dumpdata.items():
            print key, len(value)
            for item in value:
                try:
                    uuid.UUID(item.get('id'))
                    title = item.get('attributes', {}).get('title')
                    if title:
                        title = normalize(title)
                        idmap[item['id']] = title
                        item['id'] = title
                except ValueError:
                    # id is not a uuid, likely doesn't need to get fixed.
                    pass
        return (space, dumpdata, idmap)

    def _fix_dependencies(self, space, dumpdata, idmap):
        '''
        Fix the ids for items that have had their ids normalized.
        '''
        for key, value in dumpdata.items():
            target = '%s/kibana/%s/%s' % (self._directory, space, key)
            if not os.path.exists(target):
                os.makedirs(target, 0755)

            for item in value:
                count = 0
                # Fix any references that may be broken by the updated ids
                if 'references' in item:
                    for reference in item['references']:
                        if reference['id'] in idmap:
                            print reference['id'], '->', idmap[reference['id']]
                            reference['id'] = idmap[reference['id']]

                while True:
                    outpath = os.path.join(target, '%s_%d.json' % (item['id'], count))
                    if not os.path.exists(outpath):
                        json.dump(item, open(outpath, 'w'), indent=4)
                        if count > 0:
                            print outpath
                        break
                    count += 1

    def backup_space(self, space):
        '''
        Backup the kibana objects in a single space.
        '''
        self._fix_dependencies(*self._adjust_ids(space))

    def backup_kibana(self, spaces=None):
        '''
        Backup kibana saved objects in the specified spaces
        '''
        for space in self.get_spaces():
            if spaces and space not in spaces:
                continue
            self.backup_space(space)

    def backup_all(self, spaces=None):
        '''
        Backup all ELK data
        '''
        self.backup_es()
        self.backup_kibana(spaces)


def convert(data, valid=None):
    '''
    Do basic conversion from string to any of the specified types
    '''
    print data
    if not data:
        return data
    if not valid:
        valid = [float, int, str]
    for intype in valid:
        if intype == float:
            if '.' in data:
                try:
                    return intype(data)
                except ValueError:
                    pass
        else:
            try:
                return intype(data)
            except ValueError:
                pass

    # Nothing else worked, just return the data....
    return data

def main():
    '''
    Main program
    '''
    parser = argparse.ArgumentParser(
        description='Pull data from ELK in format suitable for comitting to git',
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', '--config', action='append',
                        default=[os.path.expanduser('~/.%s.cfg' % os.path.basename(sys.argv[0]).replace('.py', ''))],
                        help='''Path to configuration file, see https://docs.python.org/3/library/configparser.html
Generic parameters can be specified in the [DEFAULT] section and server specific
parameters can be specified in a section named after the server, i.e.:
[rre]
elasticsearch_host = 10.11.3.59

Values in each section are the long format of any valid command line argument.''')
    parser.add_argument('-s', '--server', default='DEFAULT',
                        help='Name of configuration section to use to connect to ELK ')
    parser.add_argument('-d', '--directory', default=None,
                        help='Name of configuration section to use to connect to ELK ')
    parser.add_argument('-e', '--elasticsearch_host', default=None,
                        help='IP address/hostname of the ELK server')
    parser.add_argument('-P', '--elasticsearch_port', default=None,
                        help='Elasticsearch server port')
    parser.add_argument('-u', '--username', default=None,
                        help='Elasticsearch user name, if required')
    parser.add_argument('-p', '--password', default=None,
                        help='Elasticsearch password, if required')
    parser.add_argument('-k', '--kibana_host', default=None,
                        help='IP address/hostname of the Kibana server, if different from elasticsearch_host')
    parser.add_argument('-K', '--kibana_port', default=None,
                        help='Kibana server port')
    parser.add_argument('-C', '--ca_certs', default=None,
                        help='Cert files to use when connecting to the ELK server ports')

    args = vars(parser.parse_args())
    config = configparser.ConfigParser()
    config.read(args.get('config'))

    # Override config values with command line arguments if set
    parameters = {}
    for key, value in config.items(args.get('server')):
        if key in args and args.get(key):
            value = args.get(key)
        parameters[key] = convert(value)
    if not args.get('directory'):
        args['directory'] = args.get('server')

    elk = ELK(directory=args.get('directory'), **parameters)
    elk.backup_all()
    sys.exit(0)


if __name__ == '__main__':
    main()
