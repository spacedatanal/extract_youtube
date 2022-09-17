import pandas as pd
import requests
import os
import datetime
import time

from googleapiclient.discovery import build
from api_tokens import tokens

class data_extraction():
    
    def __init__(self, API_KEY):
        self.API_KEY = API_KEY
        self._link_youtube = self._request_access()

    def _request_access(self):
        youtube = build('youtube', 'v3', developerKey=self.API_KEY)
        return youtube
    
    def _get_user_id(self, channel_id):
        
        youtube = self._request_access()
        request = youtube.channels().list(
                part='snippet,contentDetails,statistics,topicDetails',
                id=','.join(channel_id))
        response = request.execute() 

        data_user = pd.DataFrame(columns = ["Id","Channel", "Description", "URL", "Views", "Subscribers", "Videos", "topic_details", "related_playlist"])
        for item in response["items"]:
            
            Id = item["id"]
            channel = item["snippet"]["title"]
            description = item["snippet"]["description"]
            #country = item["snippet"]["country"]
            customURL = item["snippet"]["customUrl"]
            viewCount = item["statistics"]["viewCount"]
            subscriberCount = item["statistics"]["subscriberCount"]
            videoCount = item["statistics"]["videoCount"]
            topicDetails = item["topicDetails"]["topicCategories"]
            relatedPlaylist = item["contentDetails"]['relatedPlaylists']["uploads"]
            #PENDING TASK TO ADD COUNTRY
            
            data_user = data_user.append(
                {
                    "Id": Id,
                    "Channel": channel,
                    "Description": description,
                    #"Country": country,
                    "URL":customURL,
                    "Views": viewCount,
                    "Subscribers": subscriberCount,
                    "Videos": videoCount,
                    "topic_details": topicDetails,
                    "related_playlist": relatedPlaylist
                }, ignore_index = True
            )
            
        data_user.index = data_user.Id
        return data_user
    
    def _get_video_ids(self, playlist_id, max_days = 7):
        """
        Get list of video IDs of all videos in the given playlist
        Params:
        
        youtube: the build object from googleapiclient.discovery
        playlist_id: playlist ID of the channel
        
        Returns:
        List of video IDs of all videos in the playlist
        
        """
        youtube = self._request_access()
        request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId = playlist_id,
                    maxResults = 20)
        response = request.execute()
        
        video_ids = []
        _limit = False
        
        for i in range(len(response['items'])):
            #video_ids.append(response['items'][i]['contentDetails']['videoId'])
            _video_data = response['items'][i]["contentDetails"]["videoPublishedAt"].split("T")[0]
            if max_days == "Full":
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            else:
                dto = datetime.datetime.strptime(_video_data, '%Y-%m-%d').date()
                delta = datetime.date.today() - dto
                val = delta.days
                if val > max_days:
                    _limit = True
                else:
                   video_ids.append(response['items'][i]['contentDetails']['videoId']) 

        next_page_token = response.get('nextPageToken')
        more_pages = True

        while more_pages & (_limit==False):
            if next_page_token is None:
                more_pages = False
            else:
                youtube = self._request_access()
                request = youtube.playlistItems().list(
                            part='contentDetails',
                            playlistId = playlist_id,
                            maxResults = 50,
                            pageToken = next_page_token)
                response = request.execute()

                for i in range(len(response['items'])):
                    _video_data = response['items'][i]["contentDetails"]["videoPublishedAt"].split("T")[0]
                    if max_days == "Full":
                        video_ids.append(response['items'][i]['contentDetails']['videoId'])
                    else:
                        dto = datetime.datetime.strptime(_video_data, '%Y-%m-%d').date()
                        delta = datetime.date.today() - dto
                        val = delta.days
                        if val > max_days:
                            _limit = True
                        else:
                           video_ids.append(response['items'][i]['contentDetails']['videoId']) 

                next_page_token = response.get('nextPageToken')
                
        return video_ids
    
    def _get_video_details(self, video_ids):
        
        all_video_info = []
        
        for i in range(0, len(video_ids)):
            youtube = self._request_access()
            request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=','.join(video_ids[i:i+50])
            )
            response = request.execute() 

            for video in response['items']:
                stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                                'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                                'contentDetails': ['duration', 'definition', 'caption']
                                }
                video_info = {}
                video_info['video_id'] = video['id']

                for k in stats_to_keep.keys():
                    for v in stats_to_keep[k]:
                        try:
                            video_info[v] = video[k][v]
                        except:
                            video_info[v] = None

                all_video_info.append(video_info)
        return all_video_info
    
    def _structure_comments(self, df, i, comment):
        
        basic = comment["snippet"]["topLevelComment"]["snippet"]
        commentId = comment["snippet"]["topLevelComment"]["id"]
        videoId = i
        textDisplay = basic["textDisplay"]
        textOriginal = basic["textOriginal"]
        authorName = basic["authorDisplayName"]
        authorChannelURL = basic["authorChannelId"]["value"]
        likeCount = basic["likeCount"]
        publishedAt = basic["publishedAt"]
        updatedAt = basic["updatedAt"]
        publishedAt = publishedAt.split("T")[0]
        updatedAt = updatedAt.split("T")[0]

        data_comment = df.append(
            {
                "video_id":videoId,
                "author_id":authorChannelURL,
                "comment_id":commentId,
                "author_name":authorName,
                "test_display":textDisplay,
                "text_original":textOriginal,
                "likes_count":likeCount,
                "date_published":publishedAt,
                "date_updated":updatedAt
            }, ignore_index = True
        )
        
        return data_comment
    
    def _get_comments(self, video_ids):
        """ THIS IS TO GET COMMENTS FROM VIDEOS

        """
        more_pages = True
        data_comment = pd.DataFrame(columns = [
                                                "video_id",
                                               "author_id",
                                               "comment_id",
                                               "author_name",
                                               "test_display",
                                               "text_original",
                                               "likes_count",
                                               "date_published",
                                               "date_updated"])


        for i in video_ids:
            
            try:
                youtube = self._request_access()
                request = youtube.commentThreads().list(
                    part="Id,snippet",
                    videoId=i
                )
                response = request.execute()
                #print(i["title"], response)
                page_token = response.get('nextPageToken')

                for comment in response["items"]:

                    data_comment = self._structure_comments(data_comment, i, comment)

                while more_pages:
                    if page_token is None:
                        more_pages = False
                    else:
                        youtube = self._request_access()
                        request = youtube.commentThreads().list(
                            part="Id,snippet",
                            videoId=i,
                            pageToken = page_token
                        )
                        response = request.execute()


                        page_token = response.get('nextPageToken')

                        for comment in response["items"]:
                            data_comment = self._structure_comments(data_comment, i, comment)
            except:
                pass



        data_comment.index = data_comment.comment_id
        return data_comment
    

if __name__ == '__main__':
    param_channel_id = ["UC71sOOBgRu5UL-opaFVFstw"]
    API_KEY, = tokens().token_youtube()
    _initiation = data_extraction(API_KEY)
    data_user = _initiation._get_user_id(param_channel_id)
    for c in data_user["Channel"].unique():
        playlist_id = data_user.loc[data_user['Channel']== c, 'related_playlist'].iloc[0]
        data_playlist = _initiation._get_video_ids(playlist_id)
        data_video_ids = _initiation._get_video_details(data_playlist)
        data_comments = _initiation._get_comments(data_playlist)

