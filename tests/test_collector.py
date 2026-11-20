"""采集端验收测试：真实流式传输 + 二进制原件 + 全部异常分类。"""
import json
import threading
import urllib.request

from collector.model import Claim, ItemStatus, SessionStatus
from collector.source_client import SourceClient
from tests.collector_support import CollectorTest


def http_json(method, url, payload=None):
    data = json.dumps(payload or {}).encode()
    req = urllib.request.Request(
        url, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())


class RealStreamAndBackfillTest(CollectorTest):
    def test_real_sse_stream_and_binary_originals_complete_session(self):
        # 真实 chunked SSE：传输 id 不作业务序号；通知只含描述与摘要，不含字节
        raw = self.drain_stream()
        self.assertIn(b"event: fragment", raw)
        self.assertIn(b'"sha256"', raw)
        self.assertNotIn(b"P6", raw)
        runner = self.make_runner()
        self.poll(runner)
        checklist = runner.engine("demo-A").status()
        self.assertEqual(checklist["overall_status"], SessionStatus.COMPLETE.value)
        self.assertEqual(checklist["counts"]["items"], 9)
        self.assertEqual(checklist["counts"]["verified"], 9)
        # 二进制原件按四元组归属落盘，相机为 PPM，动作为合成二进制
        blobs = self.all_blobs_under(self.data_root)
        self.assertEqual(len(blobs), 9)
        camera = [p for p in blobs if "camera" in str(p)]
        action = [p for p in blobs if "action" in str(p)]
        self.assertTrue(any(p.read_bytes().startswith(b"P6") for p in camera))
        self.assertTrue(all(p.read_bytes().endswith(b"\x00\xff") for p in action))
        # 导出与受管理副本
        export_dir = self.export_root / "demo-A"
        self.assertTrue((export_dir / "checklist.json").exists())
        self.assertEqual(len(self.all_blobs_under(export_dir / "items")), 9)
        self.assertEqual(len(self.all_blobs_under(self.managed_root / "demo-A")), 9)

    def test_missing_seq_backfilled_via_range_api(self):
        # SSE 每通道只通知 1、3、1，序号 2 必须经 backfill 核实，不能漏也不能编
        runner = self.make_runner()
        engine = runner.engine("demo-A")
        client = runner.client
        engine.apply_manifest(client.get_manifest())
        for event in client.stream_events():
            if event["event"] == "fragment":
                engine.apply_fragment(event["data"])
        partial = engine.status()
        self.assertEqual(partial["counts"]["staged"] + partial["counts"]["verified"], 6)
        # 实时流缺序号 2：该身份尚无条目，等待补取
        self.assertNotIn("camera|1|2", engine.state.items)
        # 按 manifest 闭区间补取
        for channel in list(engine.state.channels):
            engine.backfill_channel(client, channel)
        checklist = engine.evaluate()
        self.assertEqual(checklist["overall_status"], SessionStatus.COMPLETE.value)
        self.assertEqual(self.find(checklist, "camera", 1, 2)["status"],
                         ItemStatus.VERIFIED.value)
        # 实时片段中重复的 seq=1 与补取结果同摘要，保持单条目
        self.assertEqual(self.find(checklist, "camera", 1, 1)["status"],
                         ItemStatus.VERIFIED.value)

    def test_duplicate_notifications_are_idempotent(self):
        engine = self.make_engine()
        client = SourceClient(self.base)
        engine.apply_manifest(client.get_manifest())
        for _ in range(3):
            for event in client.stream_events():
                if event["event"] == "fragment":
                    engine.apply_fragment(event["data"])
        checklist = engine.status()
        # 同一身份重复到达不产生重复条目，也不产生冲突
        self.assertEqual(checklist["counts"]["items"], 6)
        self.assertEqual(checklist["counts"]["conflict"], 0)
        camera1 = self.find(checklist, "camera", 1, 1)
        self.assertEqual(len(camera1["digests"]), 1)


class IdentityAndConflictTest(CollectorTest):
    def test_same_bytes_different_identity_kept_separately(self):
        # reconnect 后新代次从 1 重编号：同字节也必须各自归属，不能按摘要去重
        runner = self.make_runner()
        self.poll(runner)
        before = len(self.all_blobs_under(self.data_root / "demo-A" / "raw"))
        self.control("reconnect")
        self.poll(runner, times=3)
        checklist = runner.engine("demo-A").status()
        self.assertEqual(checklist["manifest_revision"], 2)
        for epoch in (1, 2):
            for channel in ("action", "camera", "annotation"):
                item = self.find(checklist, channel, epoch, 1)
                self.assertEqual(item["status"], ItemStatus.VERIFIED.value)
        after = self.all_blobs_under(self.data_root / "demo-A" / "raw")
        self.assertTrue(any("epoch1" in str(p) for p in after))
        self.assertTrue(any("epoch2" in str(p) for p in after))
        self.assertGreater(len(after), before)

    def test_conflicting_digest_isolates_channel_range(self):
        self.control("conflict")
        runner = self.make_runner()
        runner.start()  # 冲突摘要只出现在实时 SSE 中，必须真实接收
        self.wait_until(
            lambda: any(i.status == ItemStatus.CONFLICT.value
                        for i in runner.engine("demo-A")._items())
        )
        self.poll(runner, times=2)
        checklist = runner.engine("demo-A").status()
        # camera/1/1 出现两种摘要：隔离且 last-writer 不得取胜
        item = self.find(checklist, "camera", 1, 1)
        self.assertEqual(item["status"], ItemStatus.CONFLICT.value)
        self.assertEqual(len(item["digests"]), 2)
        self.assertEqual(checklist["overall_status"], SessionStatus.CONFLICTED.value)
        # 冲突项被隔离：不进入可领取 raw 命名区/managed/exports；若字节已落盘则移入 quarantine
        raw_camera1 = [p for p in self.all_blobs_under(self.data_root / "demo-A" / "raw")
                       if "camera" in str(p) and "seq1" in p.name]
        self.assertFalse(raw_camera1)
        export_names = {p.name for p in self.all_blobs_under(
            self.export_root / "demo-A" / "items")}
        self.assertFalse(any("camera" in n and "seq1" in n for n in export_names))
        managed_names = {p.name for p in self.all_blobs_under(
            self.managed_root / "demo-A")}
        self.assertFalse(any("camera" in n and "seq1" in n for n in managed_names))
        # 其他有依据的原件仍可领取
        self.assertEqual(self.find(checklist, "action", 1, 1)["claim"],
                         Claim.ALLOWED.value)


class RedactionTest(CollectorTest):
    def test_mask_purges_saved_frame_everywhere_but_keeps_tombstone(self):
        runner = self.make_runner()
        self.poll(runner)
        engine = runner.engine("demo-A")
        target = self.find(engine.status(), "camera", 1, 2)
        self.assertEqual(target["status"], ItemStatus.VERIFIED.value)
        raw_name = target["rel_path"].split("/")[-1]
        self.assertTrue(any(p.name == raw_name for p in self.all_blobs_under(self.data_root)))
        self.control("mask")
        self.poll(runner, times=2)
        checklist = engine.status()
        item = self.find(checklist, "camera", 1, 2)
        self.assertEqual(item["status"], ItemStatus.REDACTED.value)
        self.assertEqual(item["claim"], Claim.REDACTED.value)
        self.assertIsNone(item["rel_path"])
        # raw / managed / exports 三处都不再含该原件字节
        for root in (self.data_root, self.managed_root, self.export_root):
            self.assertFalse(any(p.name == raw_name for p in self.all_blobs_under(root)))
        # 整体状态为部分屏蔽，但同会话其他动作仍允许领取
        self.assertEqual(checklist["overall_status"],
                         SessionStatus.PARTIALLY_REDACTED.value)
        self.assertEqual(self.find(checklist, "action", 1, 2)["claim"],
                         Claim.ALLOWED.value)
        # 再次补取遇到 451 也只保持墓碑，不复活
        engine.backfill_channel(runner.client, "camera")
        self.assertEqual(self.find(engine.status(), "camera", 1, 2)["status"],
                         ItemStatus.REDACTED.value)

    def test_redaction_arriving_before_data_creates_tombstone(self):
        self.control("mask")
        engine = self.make_engine()
        client = SourceClient(self.base)
        engine.apply_manifest(client.get_manifest())
        engine.backfill_channel(client, "camera")
        item = self.find(engine.status(), "camera", 1, 2)
        self.assertEqual(item["status"], ItemStatus.REDACTED.value)
        self.assertIsNone(item["rel_path"])
        # 屏蔽后实时片段到达也不得复活墓碑
        for event in client.stream_events():
            if event["event"] == "fragment":
                engine.apply_fragment(event["data"])
        item = self.find(engine.status(), "camera", 1, 2)
        self.assertEqual(item["status"], ItemStatus.REDACTED.value)
        self.assertIsNone(item["rel_path"])

    def test_download_451_surface_as_redaction_not_gap(self):
        from collector.source_client import SourceRedacted
        self.control("mask")
        client = SourceClient(self.base)
        with self.assertRaises(SourceRedacted):
            client.download_original(
                {"url": "/original?session=demo-A&channel=camera&epoch=1&seq=2",
                 "sha256": "x"})

    def test_no_raw_bytes_in_logs_or_temp_files(self):
        runner = self.make_runner()
        self.poll(runner)
        self.control("mask")
        self.poll(runner, times=2)
        # 不允许残留 .part 临时拼接文件；状态 JSON 中不内嵌原始字节
        self.assertEqual(list(self.data_root.rglob("*.part")), [])
        state_text = (self.data_root / "demo-A" / "state.json").read_text(encoding="utf-8")
        self.assertNotIn("P6", state_text)
        self.assertNotIn("\\x00", state_text)


class AuthorizationAndAnchorTest(CollectorTest):
    def test_grant_window_mapped_through_anchors_only(self):
        # 授权收窄到 [0,1000]ms：相机 30ms/seq，动作 10ms/seq
        self.control("grant", session_ms=[0, 1000])
        runner = self.make_runner()
        self.poll(runner, times=2)
        checklist = runner.engine("demo-A").status()
        # action seq1=0ms 允许；seq3=2000ms 未授权
        self.assertEqual(self.find(checklist, "action", 1, 1)["claim"],
                         Claim.ALLOWED.value)
        self.assertEqual(self.find(checklist, "action", 1, 3)["claim"],
                         Claim.UNAUTHORIZED.value)
        self.assertEqual(self.find(checklist, "camera", 1, 3)["claim"],
                         Claim.UNAUTHORIZED.value)
        # 未授权件不得进 exports
        exported = {p.name for p in self.all_blobs_under(
            self.export_root / "demo-A" / "items")}
        self.assertFalse(any("seq3" in n for n in exported))
        # 授权缩小后同步清理先前已发的受管理副本
        runner.engine("demo-A").evaluate()
        managed = {p.name for p in self.all_blobs_under(
            self.managed_root / "demo-A")}
        self.assertFalse(any("seq3" in n for n in managed))

    def test_unmappable_range_keeps_gap_no_clock_substitution(self):
        # 去掉锚点后序号无法证明对应关系 -> unmapped，绝不用设备 clock 替代
        engine = self.make_engine()
        client = SourceClient(self.base)
        manifest = client.get_manifest()
        manifest["anchors"] = []
        engine.apply_manifest(manifest)
        engine.backfill_channel(client, "action")
        item = self.find(engine.status(), "action", 1, 1)
        self.assertEqual(item["status"], ItemStatus.VERIFIED.value)
        self.assertEqual(item["claim"], Claim.UNMAPPED.value)
        self.assertIsNone(item["mapped_ms"])
        self.assertEqual(engine.status()["overall_status"],
                         SessionStatus.UNDER_COLLECTED.value)

    def test_seq_outside_anchor_span_is_gap_not_extrapolation(self):
        # 锚点只覆盖 seq1..3，构造 seq5 的描述：超出锚点跨度必须 unmapped，禁止外推
        engine = self.make_engine()
        client = SourceClient(self.base)
        engine.apply_manifest(client.get_manifest())
        descriptor = client.backfill("camera", 1, 1, 3)[0]  # 取一个真实描述做模板
        descriptor = dict(descriptor, seq=5, sha256="0" * 64,
                          url="/original?session=demo-A&channel=camera&epoch=1&seq=5")
        engine.apply_fragment(descriptor)
        item = self.find(engine.status(), "camera", 1, 5)
        self.assertEqual(item["claim"], Claim.NOT_VERIFIED.value)  # 字节未核实
        self.assertIsNone(item["mapped_ms"])
        # 即使伪造为已核实，锚点跨度外仍不得映射出时间
        from collector.anchors import seq_to_session_ms
        self.assertIsNone(seq_to_session_ms(engine.state.anchor_rows, "camera", 5))

    def test_action_type_authorization(self):
        runner = self.make_runner()
        runner.request["action_types"] = ["place"]  # 来源只授予 pick
        self.poll(runner, times=2)
        checklist = runner.engine("demo-A").status()
        self.assertEqual(self.find(checklist, "action", 1, 1)["claim"],
                         Claim.UNAUTHORIZED.value)
        # 相机/注记不按动作类型过滤
        self.assertEqual(self.find(checklist, "camera", 1, 1)["claim"],
                         Claim.ALLOWED.value)


class LifecycleAndResumeTest(CollectorTest):
    def test_live_session_in_progress_and_ended_complete(self):
        self.control("live")
        runner = self.make_runner()
        self.poll(runner, times=2)
        self.assertEqual(runner.engine("demo-A").status()["overall_status"],
                         SessionStatus.IN_PROGRESS.value)
        self.control("end")
        self.poll(runner, times=2)
        self.assertEqual(runner.engine("demo-A").status()["overall_status"],
                         SessionStatus.COMPLETE.value)

    def test_unavailable_channel_stays_under_collected(self):
        runner = self.make_runner()
        self.poll(runner)
        self.control("unavailable")
        # 新代次后相机不可达：相机通道保持欠采，动作/注记不受牵连
        self.control("reconnect")
        self.poll(runner, times=3)
        checklist = runner.engine("demo-A").status()
        camera_gaps = [i for i in checklist["items"]
                       if i["channel"] == "camera" and i["epoch"] == 2
                       and i["status"] == ItemStatus.SOURCE_GAP.value]
        self.assertTrue(camera_gaps)
        self.assertEqual(checklist["overall_status"],
                         SessionStatus.UNDER_COLLECTED.value)
        self.assertTrue(checklist["last_source_error"])

    def test_resume_distinguishes_staged_from_verified(self):
        runner = self.make_runner()
        self.poll(runner)
        # 模拟崩溃：删掉一个已验证原件的字节但保留状态，再重建引擎
        target = self.find(runner.engine("demo-A").status(), "camera", 1, 3)
        runner.engine("demo-A").storage.resolve(target["rel_path"]).unlink()
        from collector.session import SessionEngine
        from collector.storage import Storage
        rebuilt_storage = Storage(
            self.data_root / "demo-A", self.managed_root, self.export_root)
        rebuilt = SessionEngine("demo-A", self.research_request, rebuilt_storage)
        item = self.find(rebuilt.status(), "camera", 1, 3)
        self.assertEqual(item["status"], ItemStatus.STAGED.value)
        # 重新补取后恢复 verified
        rebuilt.backfill_channel(runner.client, "camera")
        self.assertEqual(self.find(rebuilt.status(), "camera", 1, 3)["status"],
                         ItemStatus.VERIFIED.value)

    def test_stop_freezes_session_and_resume_works(self):
        runner = self.make_runner()
        self.poll(runner)
        engine = runner.engine("demo-A")
        engine.stop()
        self.assertEqual(engine.status()["overall_status"],
                         SessionStatus.STOPPED.value)
        # 停止期间的来源变更不得写入
        self.control("mask")
        runner._poll_once()
        self.assertEqual(self.find(engine.status(), "camera", 1, 2)["status"],
                         ItemStatus.VERIFIED.value)
        engine.resume()
        runner._poll_once()
        self.assertEqual(self.find(engine.status(), "camera", 1, 2)["status"],
                         ItemStatus.REDACTED.value)


class ApiTest(CollectorTest):
    def test_http_api_status_stop_and_backfill(self):
        from collector.api import build_server
        runner = self.make_runner()
        runner.start()
        httpd = build_server(runner, "127.0.0.1", 0)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(httpd.shutdown)
        self.addCleanup(httpd.server_close)
        port = httpd.server_port
        self.wait_until(
            lambda: runner.engines and runner.engine("demo-A").status()["counts"]["verified"] == 9
        )
        doc = self.api_get(port, "/sessions/demo-A")
        self.assertEqual(doc["overall_status"], SessionStatus.COMPLETE.value)
        stopped = http_json("POST", f"http://127.0.0.1:{port}/sessions/demo-A/stop")
        self.assertTrue(stopped["stopped"])
        resumed = http_json("POST", f"http://127.0.0.1:{port}/sessions/demo-A/resume")
        self.assertFalse(resumed["stopped"])
        info = self.api_get(port, "/sessions/demo-A/export")
        self.assertTrue(info["export_dir"].endswith("demo-A"))
        self.assertEqual(self.api_get(port, "/health")["kind"], "teaching-collector")
        # 手动触发补取接口
        refreshed = http_json("POST", f"http://127.0.0.1:{port}/sessions/demo-A/backfill")
        self.assertEqual(refreshed["overall_status"], SessionStatus.COMPLETE.value)
