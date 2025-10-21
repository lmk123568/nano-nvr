import os
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Body, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
import psutil
from pydantic import BaseModel

from scheduler import cleanup_old_videos
from utils import get_video_shanghai_time, get_zlm_secret

# =========================================================
# zlmediakit 服务器地址
ZLM_SERVER = "http://127.0.0.1:8080"
# zlmediakit 密钥
ZLM_SECRET = get_zlm_secret("/opt/zlm/conf/config.ini")
# 录像存储地址
RECORD_ROOT = Path("/opt/zlm/record/")
# 保留的视频片段数量
KEEP_VIDEOS = 72
# =========================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = AsyncIOScheduler()

    # 添加任务：每小时整点执行
    scheduler.add_job(
        cleanup_old_videos,
        kwargs={"path": RECORD_ROOT, "keep_videos": KEEP_VIDEOS},
        trigger=CronTrigger(hour=0, minute=0),  # 每小时整点
        id="cleanup_videos",
        name="每小时清理旧视频片段",
        replace_existing=True,
    )

    # 只有在这里，事件循环已经启动，可以安全 start
    scheduler.start()
    print("[Scheduler] 🚀 定时任务已启动")

    yield

    scheduler.shutdown()
    print("[Scheduler] 🛑 定时任务已取消")


t = """
| 端口  | 协议    | 服务                            |
| ----- | ------- | ------------------------------- |
| 10800 | TCP     | NanoNVR 前端                    |
| 10801 | TCP     | NanoNVR 后端                    |
| 1935  | TCP     | RTMP 推流拉流                   |
| 8080  | TCP     | FLV、HLS、TS、fMP4、WebRTC 支持 |
| 8443  | TCP     | HTTPS、WebSocket 支持           |
| 8554  | TCP     | RTSP 服务端口                   |
| 10000 | TCP/UDP | RTP、RTCP 端口                  |
| 8000  | UDP     | WebRTC ICE/STUN 端口            |
| 9000  | UDP     | WebRTC 辅助端口                 |

"""

app = FastAPI(
    title="接口",
    version="latest",
    description=t,
    lifespan=lifespan,
)

# 设置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


client = httpx.AsyncClient(
    timeout=5.0,
    limits=httpx.Limits(
        max_connections=10,
        max_keepalive_connections=20,
    ),
)

# =============================================================================


@app.get("/api/perf/statistic", summary="获取主要对象个数", tags=["性能"])
async def get_statistic():
    url = f"{ZLM_SERVER}/index/api/getStatistic"
    query = {"secret": ZLM_SECRET}
    response = await client.get(url, params=query)
    return response.json()


@app.get("/api/perf/work-threads-load", summary="获取后台线程负载", tags=["性能"])
async def get_work_threads_load():
    url = f"{ZLM_SERVER}/index/api/getWorkThreadsLoad"
    query = {"secret": ZLM_SECRET}
    response = await client.get(url, params=query)
    return response.json()


@app.get("/api/perf/threads-load", summary="获取网络线程负载", tags=["性能"])
async def get_threads_load():
    url = f"{ZLM_SERVER}/index/api/getThreadsLoad"
    query = {"secret": ZLM_SECRET}
    response = await client.get(url, params=query)
    return response.json()


@app.get(
    "/api/perf/host-stats",
    tags=["性能"],
    summary="获取当前系统资源使用率（CPU、内存、磁盘、网络）",
)
async def get_system_stats():
    timestamp = datetime.now().strftime("%H:%M:%S")

    # CPU 使用率（非阻塞）
    cpu_percent = psutil.cpu_percent(interval=None)

    # 内存
    memory = psutil.virtual_memory()
    memory_info = {
        "used": round(memory.used / (1024**3), 2),
        "total": round(memory.total / (1024**3), 2),
    }

    # 磁盘
    disk = psutil.disk_usage("/")
    disk_info = {
        "used": round(disk.used / (1024**3), 2),
        "total": round(disk.total / (1024**3), 2),
    }

    # 网络
    net = psutil.net_io_counters()
    net_info = {
        "sent": net.bytes_sent,
        "recv": net.bytes_recv,
    }

    return {
        "code": 0,
        "data": {
            "time": timestamp,
            "cpu": round(cpu_percent, 2),
            "memory": memory_info,
            "disk": disk_info,
            "network": net_info,
        },
    }


# =============================================================================


@app.get("/api/stream/streamid-list", summary="获取当前流ID列表", tags=["流"])
async def get_streamid_list(
    schema: str | None = Query(None, description="筛选协议，例如 rtsp或rtmp"),
    vhost: str | None = Query(None, description="筛选虚拟主机"),
    app: str | None = Query(None, description="筛选应用名"),
    stream: str | None = Query(None, description="筛选流id"),
):
    url = f"{ZLM_SERVER}/index/api/getMediaList"
    query = {"secret": ZLM_SECRET}

    if schema:
        query["schema"] = schema
    if vhost:
        query["vhost"] = vhost
    if app:
        query["app"] = app
    if stream:
        query["stream"] = stream

    response = await client.get(url, params=query)
    raw_data = response.json()

    if raw_data["code"] != 0:
        return raw_data  # 错误直接返回

    media_list = raw_data.get("data", [])
    stream_map = {}

    for media in media_list:
        key = (media["vhost"], media["app"], media["stream"])
        if key not in stream_map:
            # 初始化主信息（这些字段在同一个流中应该一致）
            stream_map[key] = {
                "vhost": media["vhost"],
                "app": media["app"],
                "stream": media["stream"],
                "originTypeStr": media["originTypeStr"],
                "originUrl": media["originUrl"],
                "originSock": media["originSock"],
                "aliveSecond": media["aliveSecond"],
                "isRecordingMP4": media["isRecordingMP4"],
                "isRecordingHLS": media["isRecordingHLS"],
                "totalReaderCount": media["totalReaderCount"],
                "schemas": [],
            }

        # 添加当前 schema 的信息
        stream_map[key]["schemas"].append(
            {
                "schema": media["schema"],
                "bytesSpeed": media["bytesSpeed"],
                "readerCount": media["readerCount"],
                "totalBytes": media["totalBytes"],
                "tracks": media.get("tracks", []),
            }
        )

    # 转为列表返回
    result = list(stream_map.values())
    return {"code": 0, "data": result}


class ActivePullRequest(BaseModel):
    vhost: str
    app: str
    stream: str
    url: str
    rtp_type: int
    audio_type: int
    enable_rtsp: bool
    enable_rtmp: bool
    enable_hls: bool
    enable_hls_fmp4: bool
    enable_ts: bool
    enable_fmp4: bool


@app.post("/api/stream/active-pull", tags=["流"], summary="主动拉流")
async def post_active_pull(body: ActivePullRequest = Body(...)):
    # 简单验证
    if not any(
        body.url.startswith(prefix)
        for prefix in ["rtsp://", "rtmp://", "http://", "https://"]
    ):
        return {
            "code": -1,
            "msg": "源流地址必须以 rtsp://、rtmp://、http:// 或 https:// 开头",
        }

    url = f"{ZLM_SERVER}/index/api/addStreamProxy"

    query = {"secret": ZLM_SECRET}

    query["vhost"] = str(body.vhost)
    query["app"] = str(body.app)
    query["stream"] = str(body.stream)
    query["url"] = str(body.url)
    query["rtp_type"] = str(body.rtp_type)
    query["enable_rtsp"] = str(int(body.enable_rtsp))
    query["enable_rtmp"] = str(int(body.enable_rtmp))
    query["enable_hls"] = str(int(body.enable_hls))
    query["enable_hls_fmp4"] = str(int(body.enable_hls_fmp4))
    query["enable_ts"] = str(int(body.enable_ts))
    query["enable_fmp4"] = str(int(body.enable_fmp4))

    if body.audio_type == 0:
        query["enable_audio"] = "0"
    elif body.audio_type == 1:
        query["enable_audio"] = "1"
    elif body.audio_type == 2:
        query["enable_audio"] = "1"
        query["add_mute_audio"] = "1"

    response = await client.get(url, params=query)
    print(response)
    return response.json()


@app.delete("/api/stream/streamid", tags=["流"], summary="删除流ID")
async def delete_streamid(
    vhost: str = Query(..., description="虚拟主机"),
    app: str = Query(..., description="应用名"),
    stream: str = Query(..., description="流ID"),
):
    url = f"{ZLM_SERVER}/index/api/close_streams"

    query = {"secret": ZLM_SECRET}
    query["vhost"] = str(vhost)
    query["app"] = str(app)
    query["stream"] = str(stream)
    query["force"] = "1"

    response = await client.get(url, params=query)
    return response.json()


# =============================================================================
@app.get("/api/record/start-record", tags=["录制"], summary="开启录制")
async def get_start_record(
    vhost: str = Query(..., description="虚拟主机"),
    app: str = Query(..., description="应用名"),
    stream: str = Query(..., description="流ID"),
    record_days: str = Query(..., description="录制天数"),
):
    stream_record_dir = RECORD_ROOT / app / stream

    if stream_record_dir.exists():
        return {"code": -1, "msg": "该流ID录像存在，为防止覆盖，请先删除"}

    url = f"{ZLM_SERVER}/index/api/startRecord"

    query = {"secret": ZLM_SECRET}
    query["vhost"] = str(vhost)
    query["app"] = str(app)
    query["stream"] = str(stream)
    query["type"] = "1"

    max_second = (int(record_days) * 24 * 60 * 60) / KEEP_VIDEOS
    query["max_second"] = str(max_second)

    response = await client.get(url, params=query)
    return response.json()


@app.get("/api/record/stop-record", tags=["录制"], summary="停止录制")
async def get_stop_record(
    vhost: str = Query(..., description="虚拟主机"),
    app: str = Query(..., description="应用名"),
    stream: str = Query(..., description="流ID"),
):
    url = f"{ZLM_SERVER}/index/api/stopRecord"

    query = {"secret": ZLM_SECRET}
    query["vhost"] = str(vhost)
    query["app"] = str(app)
    query["stream"] = str(stream)
    query["type"] = "1"

    response = await client.get(url, params=query)
    return response.json()


@app.get("/api/record/event-record", tags=["录制"], summary="开启事件视频录制")
async def get_event_record(
    vhost: str = Query(..., description="虚拟主机"),
    app: str = Query(..., description="应用名"),
    stream: str = Query(..., description="流ID"),
    path: str = Query(..., description="录像保存相对路径"),
    back_ms: str = Query(..., description="回溯录制时长"),
    forward_ms: str = Query(..., description="后续录制时长"),
):
    url = f"{ZLM_SERVER}/index/api/startRecordTask"

    query = {"secret": ZLM_SECRET}
    query["vhost"] = str(vhost)
    query["app"] = str(app)
    query["stream"] = str(stream)
    query["path"] = path
    query["back_ms"] = back_ms
    query["forward_ms"] = forward_ms

    response = await client.get(url, params=query)
    return response.json()


@app.get(
    "/api/record/videos-list",
    tags=["录制"],
    summary="获取所有流ID的录像信息",
)
async def get_video_list():
    result = []

    if not RECORD_ROOT.exists() or not RECORD_ROOT.is_dir():
        return {"code": -1, "msg": f"{RECORD_ROOT} 目录不存在或不是目录"}

    try:
        for app_name in os.listdir(RECORD_ROOT):
            app_path = RECORD_ROOT / app_name
            if not app_path.is_dir():
                continue

            for stream_name in os.listdir(app_path):
                stream_path = app_path / stream_name
                if not stream_path.is_dir():
                    continue

                total_slices = 0
                total_size_bytes = 0
                first_video_duration = 0
                dates = set()  # 用于收集非空的日期目录

                # 遍历 stream 下所有子目录和 .mp4 文件
                for root, dirs, files in os.walk(stream_path):
                    root_dir = Path(root)
                    dir_name = root_dir.name
                    parts = dir_name.split("-")

                    # 尝试解析目录名为 YYYY-MM-DD
                    formatted_date = None
                    if len(parts) == 3:
                        try:
                            year, month, day = (
                                int(parts[0]),
                                int(parts[1]),
                                int(parts[2]),
                            )
                            if (
                                2000 <= year <= 2100
                                and 1 <= month <= 12
                                and 1 <= day <= 31
                            ):
                                formatted_date = f"{year:04d}-{month:02d}-{day:02d}"
                        except ValueError:
                            pass

                    has_mp4_in_dir = False
                    for file in files:
                        if not file.lower().endswith(".mp4"):
                            continue

                        file_path = root_dir / file
                        if not file_path.exists():
                            continue

                        try:
                            size = file_path.stat().st_size
                            total_size_bytes += size
                            total_slices += 1
                            has_mp4_in_dir = True  # 标记此目录非空
                        except OSError:
                            continue

                        # 只在第一次提取时长
                        if first_video_duration == 0:
                            info = get_video_shanghai_time(file_path)
                            if info:
                                first_video_duration = round(
                                    info["duration"] / (86400 / KEEP_VIDEOS)
                                ) * (86400 / KEEP_VIDEOS)

                    # 如果当前目录有 .mp4 文件，且解析出有效日期，则加入 dates
                    if has_mp4_in_dir and formatted_date:
                        dates.add(formatted_date)

                # 跳过无视频的 stream
                if total_slices == 0:
                    continue

                if first_video_duration == 0:
                    record_days = "-"
                else:
                    record_days = KEEP_VIDEOS * first_video_duration / 86400

                # 构建结果
                result.append(
                    {
                        "app": app_name,
                        "stream": stream_name,
                        "slice_num": total_slices,
                        "total_storage_gb": round(total_size_bytes / (1024**3), 2),
                        "record_days": str(record_days),
                        "dates": sorted(dates),  # 按时间顺序排序输出
                    }
                )

        return {"code": 0, "data": result}

    except Exception as e:
        print(f"目录遍历异常: {e}")
        return {"code": -1, "msg": "目录遍历异常"}


@app.get("/api/record/videos", tags=["录制"], summary="获取指定流ID的全部录像信息")
async def get_video(
    app: str = Query(..., description="应用名, 如 live"),
    stream: str = Query(..., description="流ID, 如 test"),
    date: str = Query(..., description="日期格式: YYYY-MM-DD"),
):
    target_dir = RECORD_ROOT / app / stream / date

    if not target_dir.exists():
        return {"code": 1, "msg": f"目录不存在: {target_dir}"}

    if not target_dir.is_dir():
        return {"code": 1, "msg": f"路径不是目录: {target_dir}"}

    results = []

    for file_path in target_dir.iterdir():
        # print(file_path)
        if file_path.suffix.lower() == ".mp4":
            # print(f"处理文件: {file_path}")
            data = get_video_shanghai_time(file_path)
            if data:
                try:
                    # 计算相对路径：app/stream/date/filename.mp4
                    rel_path = file_path.relative_to(RECORD_ROOT)
                    # 构造 Nginx 可访问的路径
                    # nginx_path = f"/record/{rel_path}"
                    # data["filename"] = nginx_path
                    data["filename"] = str(rel_path)
                except ValueError:
                    print(f"⚠️ 文件不在 RECORD_ROOT 下，跳过: {file_path}")
                    continue

                results.append(data)

    # 按开始时间排序
    results.sort(key=lambda x: x["start"])

    return {"code": 0, "data": results}


@app.delete("/api/record/videos", tags=["录制"], summary="删除指定流ID的全部录像文件")
async def delete_recordings(
    app: str = Query(..., description="应用名, 如 live"),
    stream: str = Query(..., description="流ID, 如 test"),
):
    base_dir = RECORD_ROOT / app / stream

    if not base_dir.exists():
        return {"code": -1, "msg": f"目录不存在: {base_dir}"}

    if not base_dir.is_dir():
        return {"code": -1, "msg": f"路径不是目录: {base_dir}"}

    try:
        shutil.rmtree(base_dir)
        return {"code": 0, "msg": f"已删除整个流录像: {base_dir}"}
    except Exception as e:
        return {"code": -1, "msg": f"删除流目录失败: {str(e)}"}


# =============================================================================


@app.get("/api/server/config", tags=["配置"], summary="获取服务器配置")
async def get_server_config():
    url = f"{ZLM_SERVER}/index/api/getServerConfig"
    query_params = {"secret": ZLM_SECRET}
    response = await client.get(url, params=query_params)
    return response.json()


@app.put("/api/server/config", tags=["配置"], summary="修改服务器配置")
async def put_server_config(request: Request):
    url = f"{ZLM_SERVER}/index/api/setServerConfig"

    query_params = dict(request.query_params)
    query_params["secret"] = ZLM_SECRET

    response = await client.get(url, params=query_params)
    return response.json()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=10801, reload=True)
    # uvicorn.run("main:app", host="0.0.0.0", port=10801, reload=False)
