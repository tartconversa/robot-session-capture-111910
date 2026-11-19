import json,time,urllib.request
from tests.support import SourceTest

class StreamTest(SourceTest):
    def test_real_stream_original_and_control(self):
        start=time.monotonic()
        with urllib.request.urlopen(self.base+'/events',timeout=5) as r:
            self.assertEqual(r.headers['Transfer-Encoding'],'chunked');first=r.readline();self.assertTrue(first.startswith(b'id:'));early=time.monotonic()-start;remaining=r.read()
        elapsed=time.monotonic()-start;self.assertGreater(elapsed-early,.12);self.assertIn(b'"seq": 3',remaining)
        rows=self.get('/backfill?channel=camera&epoch=1&from=2&to=2')['items'];self.assertEqual(len(rows),1)
        status,raw,headers=self.request(rows[0]['url']);self.assertEqual(status,200);self.assertTrue(raw.startswith(b'P6'))
        self.control('mask');self.assertEqual(self.request(rows[0]['url'])[0],451);self.assertEqual(self.get('/manifest')['redactions'][0]['seq'],[2,2])
        self.control('reconnect');self.assertEqual(self.request(rows[0]['url'])[0],409)
        self.control('live');self.assertFalse(self.get('/manifest')['ended']);self.control('unavailable');self.assertEqual(self.request('/backfill?channel=camera&epoch=2')[0],503)
        self.control('grant',session_ms=[0,1000]);self.assertEqual(self.get('/manifest')['grant']['session_ms'],[0,1000])
