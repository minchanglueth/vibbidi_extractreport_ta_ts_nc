
# from create_report_topalbum import create_update_spreadsheet
from slack_sdk.errors import SlackApiError
import _locate_
import sys
sys.path.insert(1,_locate_.BASE_DIR)
from connect_confidential.connect_slack import client_slack
        
report_crawler = """Crawler's status report criteria on {}:
\ta. Total number of crawlingtasks id: {} (ta: 14, ts&nc: 10, s11: >60)
\tb. Status values: {} (should be complete only)
\tc. Failed crawlingtasksid: {}
URLs : {}"""

class send_message_slack:
    def __init__(self, autotype, count_crlid, valuestats, crlid, report):
        self.autotype = autotype
        self.report = report
        self.count_crawlingtasksid = count_crlid
        self.value_status = valuestats
        self.sql2 = crlid

    def msg_slack(self):
        # topalbum_crawler = message
        report_crawler_updated = report_crawler.format(self.autotype, self.count_crawlingtasksid, self.value_status, self.sql2, self.report)
        print(report_crawler_updated)
    
    def send_to_slack(self):
        report_crawler_updated = report_crawler.format(self.autotype, self.count_crawlingtasksid, self.value_status, self.sql2, self.report)
        try:
            # client_slack.chat_postMessage(channel='minchan-testing', text=str(report_crawler_updated)) # slack channel bên M<3
            client_slack.chat_postMessage(channel='data-auto-report', text=str(report_crawler_updated))
            # client_slack.chat_postMessage(channel='data-internal', text=str(report_crawler_updated))
        except SlackApiError as e:
        ## You will get a SlackApiError if "ok" is False
            assert e.response["ok"] is False
            assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'
            print(f"Got an error: {e.response['error']}")
            print(f"Got an error: {e.response['ok']}")

