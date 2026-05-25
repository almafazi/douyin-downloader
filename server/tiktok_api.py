"""TikTok-style REST API endpoints.

Mirrors gateway-go's /tiktok and /tiktok/download endpoints using the built-in
DouyinAPIClient (no external IPC needed). Streams media directly to client
without saving to disk.

POST /tiktok         - fetch video info, return download URLs
GET /tiktok/download - stream media with Range support and 403 refresh
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Dict, List, Optional

import aiohttp
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from utils.logger import setup_logger

logger = setup_logger("TikTokAPI")

BUFFER_SIZE = 256 * 1024


class TikTokRequest(BaseModel):
    url: str
    proxy: Optional[str] = None
    impersonate: Optional[str] = None


class TikTokAuthor(BaseModel):
    nickname: str = ""
    uniqueId: str = ""
    signature: str = ""
    avatar: str = ""
    avatarThumb: str = ""
    avatarMedium: str = ""
    avatarLarger: str = ""


class TikTokResponse(BaseModel):
    status: str
    extract_source: str = "web"
    title: str = ""
    description: str = ""
    statistics: Dict[str, Any] = {}
    artist: str = ""
    cover: str = ""
    duration: int = 0
    audio: Optional[str] = None
    download_link: Dict[str, Any] = {}
    music_duration: int = 0
    author: TikTokAuthor = TikTokAuthor()
    photos: Optional[List[Dict[str, Any]]] = None
    download_slideshow: Optional[str] = None


class DeliveryPlan(BaseModel):
    direct_url: str = ""
    request_headers: Dict[str, str] = {}
    response_headers: Dict[str, str] = {}
    media_type: str = "video/mp4"
    can_refresh: bool = True
    needs_ffmpeg: bool = False
    platform: str = "douyin"
    ffmpeg_audio_url: Optional[str] = None
    ffmpeg_audio_headers: Dict[str, str] = {}
    ffmpeg_merge: bool = False
    ffmpeg_audio_only: bool = False
    session_type: str = "video"
    delivery_mode: str = "single_progressive"
    photo_urls: list = []
    audio_url: Optional[str] = None
    duration_per_image: int = 4
    content_type: str = "video"
    fallback_proxy: bool = False
    key: str = ""
    use_worker_mp3: bool = False
    bypass_proxy: bool = True


_session_store: Dict[str, Dict[str, Any]] = {}


def _generate_key() -> str:
    return f"w1::{uuid.uuid4().hex[:12]}"


def _store_session(key: str, data: Dict[str, Any]) -> None:
    _session_store[key] = data


def _get_session(key: str) -> Optional[Dict[str, Any]]:
    return _session_store.get(key)


def _build_tiktok_response(aweme_data: Dict[str, Any]) -> TikTokResponse:
    author = aweme_data.get("author", {}) or {}
    video = aweme_data.get("video", {}) or {}
    music = aweme_data.get("music", {}) or {}
    statistics = aweme_data.get("statistics", {}) or {}

    desc = (aweme_data.get("desc", "") or "").strip()
    nickname = author.get("nickname", "")

    session_key = _generate_key()

    play_addr = video.get("play_addr", {}) or {}
    url_list = play_addr.get("url_list") or []

    no_watermark_url = ""
    for url_candidate in url_list:
        if not url_candidate:
            continue
        if "watermark=0" in url_candidate:
            no_watermark_url = url_candidate
            break
        if ("zjcdn.com" in url_candidate or "douyinvod.com" in url_candidate) and "watermark" not in url_candidate:
            no_watermark_url = url_candidate
            break

    music_play_url = music.get("play_url") if isinstance(music, dict) else None
    mp3_url = None
    if isinstance(music_play_url, dict):
        music_url_list = music_play_url.get("url_list") or []
        mp3_url = music_url_list[0] if isinstance(music_url_list, list) and music_url_list else None

    download_links: Dict[str, str] = {}
    if no_watermark_url:
        download_links["no_watermark"] = f"/tiktok/download?key={session_key}"
    if mp3_url:
        download_links["mp3"] = f"/tiktok/download?key={session_key}_mp3"

    session_data = {
        "aweme_id": aweme_data.get("aweme_id", ""),
        "desc": desc,
        "author_nickname": nickname,
        "cover": _extract_cover_url(video),
        "duration": video.get("duration", 0),
        "no_watermark_url": no_watermark_url,
        "mp3_url": mp3_url,
        "music_title": music.get("title", ""),
        "music_author": music.get("author", ""),
        "download_headers": {
            "Referer": "https://www.douyin.com/",
            "Accept-Encoding": "identity",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
    }
    _store_session(session_key, session_data)

    if mp3_url:
        mp3_session_key = f"{session_key}_mp3"
        mp3_data = session_data.copy()
        mp3_data["direct_url"] = mp3_url
        mp3_data["media_type"] = "audio/mpeg"
        mp3_data["session_type"] = "mp3"
        _store_session(mp3_session_key, mp3_data)

    author_avatar_thumb = author.get("avatar_thumb")
    if isinstance(author_avatar_thumb, dict):
        avatar_url_list = author_avatar_thumb.get("url_list") or []
        avatar_url = avatar_url_list[0] if (isinstance(avatar_url_list, list) and avatar_url_list) else ""
    else:
        avatar_url = ""

    video_duration = session_data["duration"]
    download_links["watermark"] = download_links.get("no_watermark", "")

    is_gallery = bool(aweme_data.get("images") or aweme_data.get("image_post_info"))
    cover_url = session_data["cover"]

    if is_gallery:
        return _build_gallery_response(
            aweme_data, session_key, author, nickname, desc, statistics, cover_url,
            download_links, video_duration, avatar_url, mp3_url,
        )

    return _build_video_response(
        author, nickname, desc, statistics, cover_url, video_duration, mp3_url,
        download_links, avatar_url,
    )


def _build_video_response(
    author_dict: Dict[str, Any], nickname: str, desc: str, statistics: Dict[str, Any],
    cover_url: str, video_duration: int, mp3_url: Optional[str],
    download_links: Dict[str, Any], avatar_url: str,
) -> TikTokResponse:
    return TikTokResponse(
        status="tunnel",
        extract_source="web",
        title=desc,
        description=desc,
        statistics=statistics,
        artist=nickname,
        cover=cover_url,
        duration=video_duration,
        audio=mp3_url,
        download_link=download_links,
        music_duration=video_duration,
        author=TikTokAuthor(
            nickname=nickname,
            uniqueId=author_dict.get("unique_id", ""),
            signature=author_dict.get("signature", ""),
            avatar=avatar_url,
            avatarThumb=avatar_url,
            avatarMedium=avatar_url,
            avatarLarger=avatar_url,
        ),
    )


def _iter_gallery_items(aweme_data: Dict[str, Any]) -> List[Any]:
    image_post = aweme_data.get("image_post_info")
    if isinstance(image_post, dict):
        for key in ("images", "image_list"):
            candidate = image_post.get(key)
            if isinstance(candidate, list) and candidate:
                return candidate
    images = aweme_data.get("images") or aweme_data.get("image_list") or []
    if isinstance(images, list):
        return images
    return []


def _collect_photo_candidates(img: Dict[str, Any]) -> List[str]:
    sources = [
        img.get("watermark_free_download_url_list"),
        img,
        img.get("origin_image"),
        img.get("display_image"),
        img.get("download_url"),
        img.get("download_addr"),
        img.get("download_url_list"),
        img.get("owner_watermark_image"),
    ]
    
    def extract_urls(source: Any) -> List[str]:
        if isinstance(source, dict):
            url_list = source.get("url_list") or source.get("urlList")
            if isinstance(url_list, list) and url_list:
                return [item for item in url_list if isinstance(item, str) and item]
        elif isinstance(source, list) and source:
            return [item for item in source if isinstance(item, str) and item]
        elif isinstance(source, str) and source:
            return [source]
        return []

    def is_watermarked(url: str) -> bool:
        normalized = url.lower()
        watermark_hints = (
            "tplv-dy-water",
            "dy-water",
            "owner_watermark",
            "watermark_image",
            "watermark=1",
            "playwm",
        )
        return any(hint in normalized for hint in watermark_hints)

    def media_url_priority(url: str) -> int:
        normalized = url.lower()
        from urllib.parse import urlparse
        path = (urlparse(url).path or "").lower()
        score = 100 if is_watermarked(normalized) else 0
        return score + (1 if ".webp" in path else 0)

    urls: List[str] = []
    seen: set[str] = set()
    for source in sources:
        for candidate in sorted(
            extract_urls(source),
            key=media_url_priority,
        ):
            if candidate in seen:
                continue
            seen.add(candidate)
            urls.append(candidate)
    return urls


def _build_gallery_response(
    aweme_data: Dict[str, Any], session_key: str, author_dict: Dict[str, Any],
    nickname: str, desc: str, statistics: Dict[str, Any], cover_url: str,
    download_links: Dict[str, Any], video_duration: int, avatar_url: str,
    mp3_url: Optional[str] = None,
) -> TikTokResponse:
    images = _iter_gallery_items(aweme_data)
    photos = []
    image_keys: list = []

    for i, img in enumerate(images):
        if not isinstance(img, dict):
            continue
        img_url_list = _collect_photo_candidates(img)
        if not img_url_list:
            continue
        img_url = img_url_list[0]

        img_key = f"{session_key}_photo_{i}"
        img_data = {
            "aweme_id": aweme_data.get("aweme_id", ""),
            "direct_url": img_url,
            "direct_url_candidates": img_url_list,
            "media_type": "image/jpeg",
            "session_type": "photo",
            "desc": desc,
            "cover": cover_url,
            "download_headers": {
                "Referer": "https://www.douyin.com/",
                "Accept-Encoding": "identity",
            },
            "duration": video_duration,
        }
        _store_session(img_key, img_data)
        image_keys.append(img_key)

        photos.append({
            "type": "photo",
            "url": img_url,
            "download_link": f"/tiktok/download?key={img_key}",
        })

    download_links["no_watermark"] = [
        f"/tiktok/download?key={k}" for k in image_keys
    ]

    slideshow_key = f"{session_key}_slideshow"
    slideshow_data = {
        "aweme_id": aweme_data.get("aweme_id", ""),
        "photo_urls": [p["url"] for p in photos],
        "content_type": "slideshow",
        "session_type": "slideshow",
        "media_type": "video/mp4",
        "desc": desc,
        "cover": cover_url,
        "download_headers": {
            "Referer": "https://www.douyin.com/",
            "Accept-Encoding": "identity",
        },
        "duration_per_image": 4,
        "duration": video_duration,
        "audio_url": mp3_url,
    }
    _store_session(slideshow_key, slideshow_data)

    return TikTokResponse(
        status="picker",
        extract_source="web",
        title=desc,
        description=desc,
        statistics=statistics,
        artist=nickname,
        cover=cover_url,
        duration=video_duration,
        audio=mp3_url,
        download_link=download_links,
        music_duration=0,
        author=TikTokAuthor(
            nickname=nickname,
            uniqueId=author_dict.get("unique_id", ""),
            signature=author_dict.get("signature", ""),
            avatar=avatar_url,
            avatarThumb=avatar_url,
            avatarMedium=avatar_url,
            avatarLarger=avatar_url,
        ),
        photos=photos,
        download_slideshow=f"/tiktok/download?key={slideshow_key}",
    )


def _extract_cover_url(video: Dict[str, Any]) -> str:
    cover = video.get("cover")
    if not cover:
        return ""
    if isinstance(cover, dict):
        url_list = cover.get("url_list") or cover.get("urlList") or []
        if isinstance(url_list, list) and url_list:
            first = url_list[0]
            if isinstance(first, dict):
                return first.get("url", "")
            elif isinstance(first, str):
                return first
    elif isinstance(cover, str):
        return cover
    return ""


def _build_delivery_plan(session: Dict[str, Any], key: str) -> DeliveryPlan:
    direct_url = session.get("direct_url") or session.get("no_watermark_url") or ""
    media_type = session.get("media_type", "video/mp4")
    session_type = session.get("session_type", "video")
    content_type = session.get("content_type", session_type)

    if session_type == "mp3" or content_type == "mp3":
        media_type = "audio/mpeg"
        ext = "mp3"
    elif content_type == "slideshow":
        ext = "mp4"
    else:
        ext = "mp4"

    return DeliveryPlan(
        direct_url=direct_url,
        request_headers=session.get("download_headers", {
            "Accept-Encoding": "identity",
            "Referer": "https://www.douyin.com/",
        }),
        response_headers={
            "Content-Disposition": f"attachment; filename=\"{key}.{ext}\"",
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache, no-store, must-revalidate",
        },
        media_type=media_type,
        can_refresh=(content_type != "slideshow"),
        platform="douyin",
        session_type=session_type,
        delivery_mode="single_progressive",
        content_type=content_type,
        key=key,
        bypass_proxy=True,
        photo_urls=session.get("photo_urls") or [],
        audio_url=session.get("audio_url"),
        duration_per_image=session.get("duration_per_image", 4),
    )


async def _refresh_url(session: Dict[str, Any]) -> Optional[str]:
    """Refresh video URL by re-fetching from Douyin API."""
    aweme_id = session.get("aweme_id")
    if not aweme_id:
        return None
    
    from core import DouyinAPIClient
    from auth import CookieManager
    from config import ConfigLoader
    
    try:
        config = ConfigLoader("config.yml")
        cookie_manager = CookieManager()
        cookies = config.get_cookies()
        if cookies:
            cookie_manager.set_cookies(cookies)
        else:
            cookie_manager.get_cookies()
        
        async with DouyinAPIClient(cookie_manager.get_cookies()) as api_client:
            aweme_data = await api_client.get_video_detail(aweme_id)
            if not aweme_data:
                return None
            
            video = aweme_data.get("video", {}) or {}
            play_addr = video.get("play_addr", {}) or {}
            url_list = play_addr.get("url_list") or []
            
            for url_candidate in url_list:
                if not url_candidate:
                    continue
                if "watermark=0" in url_candidate:
                    return url_candidate
                if ("zjcdn.com" in url_candidate or "douyinvod.com" in url_candidate) and "watermark" not in url_candidate:
                    return url_candidate
    except Exception as e:
        logger.error("Failed to refresh URL: %s", e)
        return None
    
    return None


def _should_refresh_on_403(body: bytes, platform: str) -> bool:
    """Check if 403 is transient (should refresh) or permanent (geo-block/captcha)."""
    if not body:
        return True
    
    body_str = body.decode("utf-8", errors="ignore").lower()
    
    permanent_errors = [
        "geo", "region", "geofence",
        "do not have permission",
        "captcha", "verify",
        "blocked", "access denied",
    ]
    
    for err in permanent_errors:
        if err in body_str:
            return False
    
    return True


async def _deliver_direct(
    plan: DeliveryPlan,
    request: Request,
    session: Dict[str, Any],
) -> StreamingResponse:
    """Stream media directly from upstream URL with Range support, fallback URLs, and 403 refresh."""
    candidates = session.get("direct_url_candidates") or []
    if not candidates and plan.direct_url:
        candidates = [plan.direct_url]

    if not candidates:
        raise HTTPException(status_code=400, detail="No media URL found")

    range_header = request.headers.get("Range")
    if_range = request.headers.get("If-Range")

    last_error_status = 502
    last_error_body = b""

    # Attempt to download from each candidate URL sequentially
    for candidate_url in candidates:
        current_headers = dict(plan.request_headers)
        if range_header and not if_range:
            current_headers["Range"] = range_header

        timeout = aiohttp.ClientTimeout(total=300)
        session_client = aiohttp.ClientSession(timeout=timeout)

        try:
            resp = await session_client.get(
                candidate_url,
                headers=current_headers,
                allow_redirects=True,
            )
            # If the candidate returns a client/server error, try next candidate
            if resp.status >= 400:
                last_error_status = resp.status
                last_error_body = await resp.read()
                resp.close()
                await session_client.close()
                logger.warning(
                    "Candidate URL failed with status %d, trying next fallback: %s",
                    resp.status,
                    candidate_url[:100],
                )
                continue

            # Successful connection (200 or 206)
            async def content_generator():
                try:
                    async for chunk in resp.content.iter_chunked(BUFFER_SIZE):
                        yield chunk
                finally:
                    resp.close()
                    await session_client.close()

            # Dynamically use Content-Type from CDN response if available
            response_content_type = resp.headers.get("Content-Type", plan.media_type)
            return StreamingResponse(
                content_generator(),
                media_type=response_content_type,
                headers={k: v for k, v in plan.response_headers.items()},
                status_code=resp.status,
            )
        except Exception as e:
            logger.error("Stream error for candidate %s: %s", candidate_url[:100], e)
            await session_client.close()
            last_error_status = 502
            last_error_body = str(e).encode()
            continue

    # If all candidates failed and refresh is supported (usually for videos), try to refresh URL
    if plan.can_refresh:
        logger.info("All candidates failed, attempting to refresh URL...")
        new_url = await _refresh_url(session)
        if new_url:
            logger.info("URL refreshed successfully: %s", new_url[:100])
            session_client = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
            try:
                current_headers = dict(plan.request_headers)
                if range_header and not if_range:
                    current_headers["Range"] = range_header
                resp = await session_client.get(new_url, headers=current_headers, allow_redirects=True)
                if resp.status < 400:
                    async def content_generator():
                        try:
                            async for chunk in resp.content.iter_chunked(BUFFER_SIZE):
                                yield chunk
                        finally:
                            resp.close()
                            await session_client.close()
                    return StreamingResponse(
                        content_generator(),
                        media_type=plan.media_type,
                        headers={k: v for k, v in plan.response_headers.items()},
                        status_code=resp.status,
                    )
                else:
                    last_error_status = resp.status
                    last_error_body = await resp.read()
                    resp.close()
                    await session_client.close()
            except Exception as e:
                logger.error("Stream error after refresh: %s", e)
                await session_client.close()

    return StreamingResponse(
        iter([last_error_body]),
        media_type=plan.media_type if last_error_status != 502 else "text/plain",
        status_code=last_error_status,
    )

    raise HTTPException(status_code=502, detail="Stream failed after refresh")


async def _stream_slideshow(
    session_data: Dict[str, Any],
    plan: DeliveryPlan,
) -> StreamingResponse:
    """Download photos, render MP4 via ffmpeg, stream to client, cleanup temp files."""
    import os
    import subprocess
    import tempfile
    from imageio_ffmpeg import get_ffmpeg_exe

    photo_urls = session_data.get("photo_urls") or []
    audio_url = session_data.get("audio_url") or plan.audio_url
    duration_per_image = plan.duration_per_image or 4
    ffmpeg_exe = get_ffmpeg_exe()

    temp_dir = tempfile.mkdtemp(prefix="douyin_slideshow_")
    output_path = os.path.join(temp_dir, "slideshow.mp4")

    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Download all images
            image_paths = []
            for i, url in enumerate(photo_urls):
                img_path = os.path.join(temp_dir, f"image_{i}.jpg")
                try:
                    async with session.get(url, headers={
                        "Referer": "https://www.douyin.com/",
                        "Accept-Encoding": "identity",
                    }) as resp:
                        if resp.status == 200:
                            with open(img_path, "wb") as f:
                                async for chunk in resp.content.iter_chunked(BUFFER_SIZE):
                                    f.write(chunk)
                            image_paths.append(img_path)
                except Exception as exc:
                    logger.error("Failed to download image %d: %s", i, exc)
                    return StreamingResponse(
                        iter([b'{"error":"Failed to download slideshow image"}']),
                        media_type="application/json", status_code=502,
                    )

            # Download audio if present
            audio_path = None
            if audio_url:
                audio_path = os.path.join(temp_dir, "audio.mp3")
                try:
                    async with session.get(audio_url, headers={
                        "Referer": "https://www.douyin.com/",
                        "Accept-Encoding": "identity",
                    }) as resp:
                        if resp.status == 200:
                            with open(audio_path, "wb") as f:
                                async for chunk in resp.content.iter_chunked(BUFFER_SIZE):
                                    f.write(chunk)
                except Exception as exc:
                    logger.warning("Failed to download slideshow audio: %s", exc)
                    audio_path = None

            # Build ffmpeg command
            total_duration = len(image_paths) * duration_per_image
            ffmpeg_args = [
                ffmpeg_exe, "-y", "-hide_banner", "-loglevel", "error",
            ]
            for img in image_paths:
                ffmpeg_args.extend(["-loop", "1", "-t", str(duration_per_image), "-i", img])
            if audio_path:
                ffmpeg_args.extend(["-stream_loop", "-1", "-i", audio_path])

            # Build filter_complex
            video_streams = []
            for i in range(len(image_paths)):
                video_streams.append(
                    f"[{i}:v]scale=w=720:h=1280:force_original_aspect_ratio=decrease,"
                    f"pad=720:1280:(ow-iw)/2:(oh-ih)/2:color=black,setsar=1,"
                    f"fps=24,trim=duration={duration_per_image},setpts=PTS-STARTPTS[v{i}]"
                )
            concat_inputs = "".join(f"[v{i}]" for i in range(len(image_paths)))
            filter_parts = ";".join(video_streams)
            filter_parts += f";{concat_inputs}concat=n={len(image_paths)}:v=1:a=0[vout]"

            if audio_path:
                filter_parts += (
                    f";[{len(image_paths)}:a]atrim=0:{total_duration},asetpts=PTS-STARTPTS[aout]"
                )

            ffmpeg_args.extend([
                "-filter_complex", filter_parts,
                "-map", "[vout]",
            ])
            if audio_path:
                ffmpeg_args.extend(["-map", "[aout]", "-c:a", "aac", "-b:a", "128k"])

            ffmpeg_args.extend([
                "-pix_fmt", "yuv420p", "-fps_mode", "cfr",
                "-c:v", "libx264", "-preset", "ultrafast", "-tune", "stillimage",
                "-crf", "28", "-b:v", "320k", "-maxrate", "360k", "-bufsize", "720k",
                "-threads", "1", "-max_muxing_queue_size", "1024",
                output_path,
            ])

            # Run ffmpeg
            try:
                proc = await asyncio.create_subprocess_exec(
                    *ffmpeg_args,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                )
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=90,
                )
                if proc.returncode != 0:
                    err = stderr.decode("utf-8", errors="replace")[-500:]
                    logger.error("ffmpeg failed: %s", err)
                    return StreamingResponse(
                        iter([f'{{"error":"ffmpeg encode failed","detail":"{err}"}}'.encode()]),
                        media_type="application/json", status_code=502,
                    )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                return StreamingResponse(
                    iter([b'{"error":"ffmpeg timed out"}']),
                    media_type="application/json", status_code=504,
                )

            # Stream MP4 to client
            file_size = os.path.getsize(output_path)
            file_handle = open(output_path, "rb")

            async def stream_mp4():
                try:
                    while True:
                        chunk = file_handle.read(BUFFER_SIZE)
                        if not chunk:
                            break
                        yield chunk
                finally:
                    file_handle.close()
                    try:
                        import shutil
                        shutil.rmtree(temp_dir, ignore_errors=True)
                    except Exception:
                        pass

            return StreamingResponse(
                stream_mp4(),
                media_type="video/mp4",
                headers={
                    "Content-Disposition": f"attachment; filename=\"slideshow.mp4\"",
                    "Content-Length": str(file_size),
                    "X-Accel-Buffering": "no",
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                },
            )

    except Exception as exc:
        logger.error("Slideshow error: %s", exc)
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
        return StreamingResponse(
            iter([f'{{"error":"Slideshow failed","detail":"{exc}"}}'.encode()]),
            media_type="application/json", status_code=502,
        )


def register_tiktok_routes(app: FastAPI) -> None:
    """Register /tiktok and /tiktok/download routes on the FastAPI app."""

    @app.post("/tiktok")
    async def handle_tiktok(req: TikTokRequest) -> TikTokResponse:
        """Fetch video info and return download URLs (gateway-go compatible)."""
        if not req.url:
            return Response(content='{"error":"URL is required"}', media_type="application/json", status_code=400)

        from core import DouyinAPIClient, URLParser
        from auth import CookieManager
        from config import ConfigLoader
        from utils.validators import is_short_url, normalize_short_url

        config = ConfigLoader("config.yml")
        cookie_manager = CookieManager()
        cookies = config.get_cookies()
        if cookies:
            cookie_manager.set_cookies(cookies)
        else:
            cookie_manager.get_cookies()

        try:
            async with DouyinAPIClient(cookie_manager.get_cookies()) as api_client:
                url = req.url

                if is_short_url(url):
                    resolved = await api_client.resolve_short_url(normalize_short_url(url))
                    if not resolved:
                        return Response(content='{"error":"Failed to resolve short URL"}', media_type="application/json", status_code=400)
                    url = resolved

                parsed = URLParser.parse(url)
                if not parsed:
                    return Response(content='{"error":"Unsupported URL"}', media_type="application/json", status_code=400)

                aweme_id = parsed.get("aweme_id")
                if not aweme_id:
                    return Response(content='{"error":"No aweme_id found"}', media_type="application/json", status_code=400)

                aweme_data = await api_client.get_video_detail(aweme_id)
                if not aweme_data:
                    return Response(content='{"error":"Video not found"}', media_type="application/json", status_code=404)

                return _build_tiktok_response(aweme_data)

        except Exception as exc:
            logger.error("TikTok endpoint error: %s", exc)
            return Response(content=f'{{"error":"download_error","detail":"{exc}"}}', media_type="application/json", status_code=500)

    @app.get("/tiktok/download")
    async def handle_tiktok_download(request: Request) -> Response:
        """Stream media or return delivery plan (gateway-go compatible)."""
        key = request.query_params.get("key", "")
        _raw = request.query_params.get("download", "true").strip().lower()
        if _raw in ("0", "false", "no", "off"):
            download = False
        else:
            download = True

        if not key:
            return Response(content='{"error":"Missing key query parameter"}', media_type="application/json", status_code=400)

        session = _get_session(key)
        if not session:
            return Response(content='{"error":"Session not found or expired"}', media_type="application/json", status_code=404)

        content_type = session.get("content_type") or "video"

        if content_type == "slideshow" and download:
            plan = _build_delivery_plan(session, key)
            return await _stream_slideshow(session, plan)

        plan = _build_delivery_plan(session, key)

        if not download:
            return Response(
                content=plan.model_dump_json(),
                media_type="application/json",
            )

        return await _deliver_direct(plan, request, session)