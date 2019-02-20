#! /usr/bin/env python

'Module to interact with slack to handle bot location queries'

import os
import os.path
import time
import cPickle
import urllib3
from slackclient import SlackClient
urllib3.disable_warnings()

def runcheck(slacktoken=None, savefile='users.pkl', verbose=True):
    '''
    Determine if any users have been added or removed from the slack member list
    '''
    if not slacktoken and 'SLACK_TOKEN' in os.environ:
        slacktoken = os.environ.get('SLACK_TOKEN')

    slack_client = SlackClient(slacktoken)
    if not slack_client.rtm_connect():
        return None
    users = slack_client.api_call("users.list")
    current_users = []
    for current_item in users['members']:
        # Generate a key that should change no matter what the disable strategy....
        user_info = '%s rn=%s del=%s iur=%s ir=%s' % (
            current_item.get('name'), current_item.get('real_name'),
            current_item.get('deleted'), current_item.get('is_ultra_restricted'),
            current_item.get('is_restricted'))
        current_users.append(user_info)

    added = []
    removed = []
    if os.path.exists(savefile):
        lastusers = cPickle.load(open(savefile))
        last_user_set = set(lastusers)
        current_user_set = set(current_users)
        added = list(current_user_set.difference(last_user_set))
        removed = list(last_user_set.difference(current_user_set))
        if verbose and added:
            print 'Added:'
            for current_item in added:
                print '   ', current_item
        if verbose and removed:
            print 'Removed:'
            for current_item in removed:
                print '   ', current_item
        if added or removed:
            os.rename(savefile, '%s.%.2f' % (savefile, time.time()))
    cPickle.dump(current_users, open(savefile, 'w'))
    return added, removed

if __name__ == "__main__":
    runcheck()
