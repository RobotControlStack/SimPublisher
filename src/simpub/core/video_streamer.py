from __future__ import annotations
import time
from typing import Dict, Optional, TypedDict
import pyzlc
import cv2

from .simpub_server import ServerBase
from .utils import XRNodeInfo
from .utils import ZLC_GROUP_NAME


class VideoStreamConfig(TypedDict):
    name: str
    url: str
    width: int
    height: int

class VideoFrame(TypedDict):
    width: int  # Width of the video frame
    height: int  # Height of the video frame
    image: bytes  # Raw image data in bytes format (e.g., JPEG or PNG)
    timestamp: float  # Timestamp of the frame in seconds

class VideoStreamer:
    def __init__(self, video_source_topic: str, width: int, height: int):
        self.video_source_topic = video_source_topic
        self.width = width
        self.height = height
        self.video_publisher = pyzlc.Publisher(video_source_topic, group_name=ZLC_GROUP_NAME,)
        self.config  = VideoStreamConfig(
            name=video_source_topic,
            url=self.video_publisher.url,
            width=width,
            height=height,
        )
        self.is_streaming = False

    def update_cv_image(self, image: cv2.typing.MatLike) -> None:
        # Convert OpenCV image to the expected format
        _, buffer = cv2.imencode(".jpg", image)
        video_frame = VideoFrame(
            width=self.width,
            height=self.height,
            image=buffer.tobytes(),
            timestamp=time.time()
        )
        self._update_image(video_frame)

    def _update_image(self, image: VideoFrame):
        self.video_publisher.publish(image)

    def stop_stream(self):
        if self.is_streaming:
            print("Stopping video stream...")
            self.is_streaming = False
        else:
            print("Video stream is not running.")

class VideoStreamerManager(ServerBase):
    def __init__(self, ip_addr: str = "127.0.0.1"):
        self.streamers: Dict[str, VideoStreamer] = {}
        self._announced_streams_by_device: Dict[str, set[str]] = {}
        super().__init__("VideoStreamerManager", ip_addr)

    def initialize(self) -> None:
        pass

    def _device_key(self, xr_info: XRNodeInfo) -> str:
        return xr_info.get("nodeID", xr_info.get("name", "unknown"))

    def _mark_announced(self, xr_info: XRNodeInfo, video_source_topic: str) -> None:
        device_key = self._device_key(xr_info)
        if device_key not in self._announced_streams_by_device:
            self._announced_streams_by_device[device_key] = set()
        self._announced_streams_by_device[device_key].add(video_source_topic)

    def _already_announced(self, xr_info: XRNodeInfo, video_source_topic: str) -> bool:
        device_key = self._device_key(xr_info)
        return video_source_topic in self._announced_streams_by_device.get(device_key, set())

    def _notify_device_about_stream(self, xr_info: XRNodeInfo, streamer: VideoStreamer) -> None:
        if self._already_announced(xr_info, streamer.video_source_topic):
            return
        pyzlc.call(
            f"{xr_info['name']}/SpawnVideoReceiver",
            streamer.config,
            group_name=ZLC_GROUP_NAME,
        )
        self._mark_announced(xr_info, streamer.video_source_topic)

    async def _notify_device_about_stream_async(self, xr_info: XRNodeInfo, streamer: VideoStreamer) -> None:
        if self._already_announced(xr_info, streamer.video_source_topic):
            return
        await pyzlc.async_call(
            f"{xr_info['name']}/SpawnVideoReceiver",
            streamer.config,
            group_name=ZLC_GROUP_NAME,
        )
        self._mark_announced(xr_info, streamer.video_source_topic)

    def create_streamer(self, video_source_topic: str, width: int, height: int) -> VideoStreamer:
        if video_source_topic in self.streamers:
            print(f"Video streamer for topic '{video_source_topic}' already exists.")
            return self.streamers[video_source_topic]
        else:
            print(f"Creating new video streamer for topic '{video_source_topic}'.")
            streamer = VideoStreamer(video_source_topic, width, height)
            self.streamers[video_source_topic] = streamer
            # Notify already-connected XR devices about this new streamer
            # (handles the case where device was discovered before streamer was created)
            for xr_info in pyzlc.get_nodes_info(ZLC_GROUP_NAME):
                if not xr_info["name"].startswith("IRIS/Device/"):
                    continue
                try:
                    self._notify_device_about_stream(xr_info, streamer)
                except Exception as e:
                    print(f"Failed to notify {xr_info['name']} about video stream '{video_source_topic}': {e}")
            return streamer

    def get_streamer(self, video_source_topic: str) -> Optional[VideoStreamer]:
        return self.streamers.get(video_source_topic, None)

    def stop_all_streams(self):
        for topic, streamer in self.streamers.items():
            print(f"Stopping stream for topic '{topic}'.")
            streamer.stop_stream()
    
    async def on_new_device_found(self, xr_info: XRNodeInfo):
        # This manager does not handle XR devices, but we can log the event
        print(f"New XR device found: {xr_info.get('name', 'Unknown')}")
        for topic, streamer in self.streamers.items():
            print(f"Current video stream topic: '{topic}'")
            try:                
                await self._notify_device_about_stream_async(xr_info, streamer)
            except Exception as e:
                print(f"Error notifying XR device '{xr_info.get('name', 'Unknown')}' about video streamer topic '{topic}': {e}")
                pyzlc.error(f"Error notifying XR device '{xr_info.get('name', 'Unknown')}' about video streamer topic '{topic}': {e}")
        
