import unittest
from nifiapi.nifiapi import NifiApi
import logging
import json


class Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nifiapi = NifiApi('http://localhost:8080/nifi-api')
        logging.basicConfig(format='[%(filename)s:%(lineno)s - %(funcName)s() ] %(message)s')
        cls.logger = logging.getLogger('test_nifiapi')
        cls.logger.setLevel(logging.ERROR)

    def test_load_template_from_file(self):
        root = self.nifiapi.load_template_from_file('templates/Friendbuy.xml')
        self.assertIsNotNone(root, 'Unable to load xml template')
        id = root.find('groupId')
        self.logger.debug('groupId: %s', id.text)
        self.assertIsNotNone(id, 'Could not find element groupId')
        self.assertEqual('4d908dd7-0157-1000-c1f3-366f70148660', id.text, 'Group id has unexpected value')

    def test_find_pg(self):
        process_group = self.nifiapi.find_process_group('WebCrawler')
        self.assertIsNotNone(process_group, "Could not find process group.")
        self.assertEqual('WebCrawler', process_group['status']['name'])
        self.assertEqual('d7a613ec-1009-1157-95af-12f2fd632076', process_group['status']['id'])

    def test_find_processor_by_pg_and_name(self):
        webhook_url = 'https://hooks.slack.com/services/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'
        channel = 'nifi-notifications'
        process_group = self.nifiapi.find_process_group('Twitter PubSub Testing')
        processor = self.nifiapi.find_processor_by_pg(process_group['status']['id'], 'PutSlack')
        self.assertIsNotNone(processor)
        self.logger.debug("processors: {}".format(json.dumps(processor)))
        self.assertEqual('PutSlack', processor['status']['name'])
        processor['component']['config']['properties']['webhook-url'] = webhook_url
        processor['component']['config']['properties']['channel'] = channel
        self.logger.debug("PROCESSOR ID {}".format(processor['id']))
        new_processor = self.nifiapi.remote_put_data('/processors/{}'.format(processor['id']), processor)
        self.assertIsNotNone(new_processor, 'remote_post_data returned None')
        self.logger.debug("New Processor {}".format(json.dumps(new_processor)))
        # The webhook is a 'sensitive' value, so querying it will not return the actual value. Just
        # verify it isn't null.
        self.assertIsNotNone(new_processor['component']['config']['properties']['webhook-url'])
        self.assertEqual(channel, new_processor['component']['config']['properties']['channel'])

if __name__ == "__main__":
    unittest.main()
