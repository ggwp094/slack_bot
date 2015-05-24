#!/usr/bin/env python3

from slacker import Slacker
from configparser import ConfigParser
from pymongo import MongoClient
import sys
import json


def test_api():
    slack.api.test()
    slack.auth.test()


def join_all_channels():
    # Note: this isn't going to work with bot users
    response = slack.channels.list()
    for channel in response.body['channels']:
        if not channel['is_member']:
            slack.channels.join(channel['name'])


def store_history(chan_id, oldest=None):
    # Todo: add some cheking for overwriting here
    # Todo: add debug output
    def add_chan_id(d):
        d['channel_id'] = chan_id
        return d

    response = None
    latest = None
    collection = db['history']
    while response is None or response.body["has_more"]:
        response = slack.channels.history(
            chan_id, latest=latest, oldest=oldest)
        messages = response.body['messages']
        if not messages:
            continue
        latest = messages[-1]['ts']
        collection.insert_many(list(map(add_chan_id, messages)))


def store_all_history():
    collection = db['channels']
    response = slack.channels.list()
    channels = response.body['channels']
    collection.insert_many(
        list(map(lambda d: {'id': d['id'], 'name': d['name']}, channels)))
    for channel in channels:
        store_history(channel['id'])


def update_history():
    # Todo: проверь, что не делаешь херни с сортировкой
    # Todo: добавь индексы в бд
    # Todo: фикс случая, когда в бд нет ни одной записи на счёт канала
    collection = db['channels']
    response = slack.channels.list()
    channels = response.body['channels']
    for channel in channels:
        if collection.find_one({'id': channel['id']}) is None:
            collection.insert_one(
                {'id': channel['id'], 'name': channel['name']})
            store_history(channel['id'])
        else:
            cursor = db['history'].find(
                modifiers={'$orderby': {"ts": -1}}, limit=1, projection=['ts'])
            ts = cursor.next()['ts']
            cursor.close()
            store_history(channel['id'], ts)


commands = {
    'test': test_api,
    'join_all': join_all_channels,
    'dump': store_all_history,
    'update': update_history,
    #'users': get_users
}

# Todo: be aware of timeouts
if __name__ == '__main__':
    config = ConfigParser()
    config.read('config.ini')
    slack = Slacker(config['slack']['token'])
    db_client = MongoClient(config['mongo']['URI'])
    db = db_client[config['mongo']['database']]

    if sys.argv[1] not in commands.keys():
        print("Incorrect command")
    else:
        commands[sys.argv[1]]()
