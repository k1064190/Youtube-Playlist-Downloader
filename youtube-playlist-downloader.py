import argparse
import yt_dlp
import schedule
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


class PlaylistDownloader:
    def __init__(self, channel_url, download_path, max_workers=4):
        self.channel_url = channel_url
        self.download_path = download_path
        self.max_workers = max_workers
        self.channel_id = self.get_channel_id()

        print(f"Downloading playlists from channel URL: {self.channel_url}")

    def get_channel_id(self):
        ydl_opts = {
            'extract_flat': True,
            'skip_download': True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                channel_info = ydl.extract_info(self.channel_url, download=False)
                if 'id' in channel_info:
                    return channel_info['id']
                else:
                    print("No 'id' found in channel info.")
            except Exception as e:
                print(f"Error extracting channel id: {e}")
        return None

    def get_channel_playlists(self):
        ydl_opts = {
            'extract_flat': True,
            'skip_download': True,
            # 'verbose': True,
        }
        playlist_url = f"https://www.youtube.com/{self.channel_id}/playlists"
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            try:
                channel_info = ydl.extract_info(playlist_url, download=False)
                if 'entries' in channel_info:
                    playlists = [entry for entry in channel_info['entries'] if entry.get('_type') == 'url']
                    if playlists:
                        print(f"Successfully extracted {len(playlists)} playlists.")
                        return playlists
                    else:
                        print("No playlists found in the channel.")
                else:
                    print("No 'entries' found in channel info.")
            except Exception as e:
                print(f"Error extracting playlists from channel: {e}")
        return []

    def load_downloaded_videos(self, playlist_path):
        downloaded_videos = set()
        downloaded_videos_file = os.path.join(playlist_path, 'downloaded_videos.txt')
        if os.path.exists(downloaded_videos_file):
            with open(downloaded_videos_file, 'r') as f:
                downloaded_videos = set(line.strip() for line in f)
        return downloaded_videos

    def save_downloaded_video(self, playlist_path, video_id):
        downloaded_videos_file = os.path.join(playlist_path, 'downloaded_videos.txt')
        with open(downloaded_videos_file, 'a') as f:
            f.write(f"{video_id}\n")

    def download_video(self, video, ydl_opts, playlist_path, downloaded_videos):
        if video['id'] not in downloaded_videos:
            print(f"Attempting to download: {video['title']}")
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([video['original_url']])
                self.save_downloaded_video(playlist_path, video['id'])
                print(f"Download completed: {video['title']}")
                return True
            except yt_dlp.utils.DownloadError as e:
                if "Video unavailable" in str(e):
                    print(f"Video is unavailable: {video['title']}. Skipping to next video.")
                else:
                    print(f"Error occurred while downloading video: {e}")
                    print("Skipping this video and moving to the next one.")
            except Exception as e:
                print(f"Unexpected error occurred: {e}")
                print("Skipping this video and moving to the next one.")
        return False

    def download_playlist(self, playlist_url, playlist_title):
        playlist_path = os.path.join(self.download_path, playlist_title)
        os.makedirs(os.path.join(playlist_path, 'video'), exist_ok=True)
        os.makedirs(os.path.join(playlist_path, 'audio'), exist_ok=True)

        downloaded_videos = self.load_downloaded_videos(playlist_path)

        ydl_opts_video = {
            'outtmpl': os.path.join(playlist_path, 'video', '%(title)s.%(ext)s'),
            'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            'concurrent_fragment_downloads': 5,
            'retries': 10,
            'fragment_retries': 10,
            'skip_unavailable_fragments': True,
            'ignoreerrors': True,
            # 'verbose': True,
        }

        ydl_opts_audio = {
            'outtmpl': os.path.join(playlist_path, 'audio', '%(title)s.%(ext)s'),
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }],
            'concurrent_fragment_downloads': 5,
            'retries': 10,
            'fragment_retries': 10,
            'skip_unavailable_fragments': True,
            'ignoreerrors': True,
            # 'verbose': True,
        }

        with yt_dlp.YoutubeDL(ydl_opts_video) as ydl_video:
            try:
                playlist_dict = ydl_video.extract_info(playlist_url, download=False)

                if 'entries' not in playlist_dict:
                    print(f"No videos found in playlist: {playlist_title}")
                    return

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    video_futures = [
                        executor.submit(self.download_video, video, ydl_opts_video, playlist_path, downloaded_videos)
                        for video in playlist_dict['entries'] if video is not None]
                    audio_futures = [
                        executor.submit(self.download_video, video, ydl_opts_audio, playlist_path, downloaded_videos)
                        for video in playlist_dict['entries'] if video is not None]

                    for future in as_completed(video_futures + audio_futures):
                        future.result()

            except Exception as e:
                print(f"Error occurred while processing playlist {playlist_title}: {e}")
                print("Moving to the next playlist.")

    def download_all_playlists(self):
        # Get channel id from channel url
        playlists = self.get_channel_playlists()
        if not playlists:
            print("No playlists found in the channel.")
            return
        for playlist in playlists:
            print(f"Downloading playlist: {playlist['title']}")
            self.download_playlist(playlist['url'], playlist['title'])


def parse_args():
    parser = argparse.ArgumentParser(description="Download all videos in a YouTube channel's playlists")
    parser.add_argument("--channel_url", help="YouTube channel URL", required=True)
    parser.add_argument("--download_path", help="Path to download videos", default="playlists")
    parser.add_argument("--period", help="Period to check for new videos (in hours)", type=int, default=24)
    parser.add_argument("--max_workers", help="Maximum number of concurrent downloads", type=int, default=4)
    return parser.parse_args()


def main():
    args = parse_args()
    channel_url = args.channel_url
    download_path = args.download_path
    period = args.period
    max_workers = args.max_workers

    downloader = PlaylistDownloader(channel_url, download_path, max_workers)

    def job():
        print("Checking for new videos in playlists...")
        downloader.download_all_playlists()

    job()
    schedule.every(period).hours.do(job)

    while True:
        schedule.run_pending()
        time.sleep(3600)


if __name__ == "__main__":
    main()