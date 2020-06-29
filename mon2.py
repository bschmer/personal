#! /home/bryan/bot/.ve/bin/python

'Module to interact with slack to handle bot location queries'

import os
import os.path
import urllib3
import configparser
urllib3.disable_warnings()
import time
from slackclient import SlackClient
import cPickle


# instantiate Slack client
SLACK_CLIENT = SlackClient(os.environ['SLACK_TOKEN'])
# starterbot's user ID in Slack: value is assigned after the bot starts up
STARTERBOT_ID = None

# constants
RTM_READ_DELAY = 1 # 1 second delay between reading from RTM
EXAMPLE_COMMAND = "do"
MENTION_REGEX = "^<@(|[WU].+?)>(.*)"

def cmpdict(dict1, dict2, skip=[]):
    skipset = set(skip)
    keys1 = set(dict1.keys()).difference(skipset)
    keys2 = set(dict2.keys()).difference(skipset)
    removedkeys = keys1.difference(keys2)
    addedkeys = keys2.difference(keys1)
    samekeys = keys1.intersection(keys2)

    removed = dict([(k,dict1[k]) for k in removedkeys])
    added = dict([(k,dict2[k]) for k in addedkeys])
    changed = {}
    for k in samekeys:
        if dict1[k] != dict2[k]:
            if isinstance(dict1[k], dict) and isinstance(dict2[k], dict):
                rv = cmpdict(dict1[k], dict2[k], skip=skip)
                if rv:
                    changed[k] = rv
            else:
                changed[k] = '%s -> %s' % (dict1[k], dict2[k])
    rv = {}
    if added:
        rv['added'] = added
    if removed:
        rv['removed'] = removed
    if changed:
        rv['changed'] = changed
    return rv         

if __name__ == "__main__":
    savefile = 'users.pkl'
    if SLACK_CLIENT.rtm_connect():
        users = SLACK_CLIENT.api_call("users.list")
        curusers = {}
        for item in users['members']:
            curusers[item.get('id')] = dict(name=item.get('name'), real_name=item.get('real_name'), deleted=item.get('deleted'), ultra_resticted=item.get('is_ultra_restricted'), restricted=item.get('is_restricted'))
        if os.path.exists(savefile):
            lastusers = cPickle.load(open(savefile))
            lset = set(lastusers.keys())
            cset = set(curusers.keys())
            added = sorted(cset.difference(lset))
            removed = sorted(lset.difference(cset))
            same = sorted(lset.intersection(cset))
            slackmsg = ''
            if added:
                slackmsg += 'Added:\n'
                for item in added:
                    slackmsg += '    %s\n' % curusers[item]
            if removed:
                slackmsg += 'Removed:\n'
                for item in removed:
                    slackmsg +=  '    %s\n' % lastusers[item]
            modified = {}
            for item in same:
                rv = cmpdict(lastusers[item], curusers[item])
                if rv:
                    # Shouldn't be any added or removed since the keys are static, but changed is another option
                    msg = ''
                    for k, v in sorted(rv['changed'].items()):
                        msg += '%s: %s' % (k, v)
                    modified[item] = msg
            if modified:
                slackmsg += 'Modified:\n'
                for item in sorted(modifed.keys()):
                    slackmsg += '    %s: %s' % (item, modified[item])
            if slackmsg:
                print slackmsg
            else:
                slackmsg = 'No changes'
            SLACK_CLIENT.api_call("chat.postMessage", channel="@" + os.environ['SLACK_USER'], text=slackmsg, username='pybot', icon_emoji=':robot_face:')
            if added or removed or modified:
                os.rename(savefile, '%s.%.2f' % (savefile, time.time()))
        cPickle.dump(curusers, open(savefile, 'w'))
