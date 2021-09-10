# -*- coding: utf-8 -*-
"""find twitch midair: community edition streams for shazbuckbot"""
import requests

TWITCH_GAME_ID = "517069"  # midair community edition


class TwitchStreams:

    def __init__(self, twitch_client_id, twitch_client_secret) -> None:
        """Initialize the connection to twitch.tv

        :param str twitch_client_id: The twitch client id
        :param str twitch_client_secret: The twitch client secret
        """
        self.twitch_client_id = twitch_client_id
        self.twitch_client_secret = twitch_client_secret
        self.twitch_access_token = ''

    def get_token(self) -> str:
        """Get a new OAuth client acccess token

        :return: A OAuth client access token
        """
        url = (f'https://id.twitch.tv/oauth2/token?client_id={self.twitch_client_id}'
               f'&client_secret={self.twitch_client_secret}&grant_type=client_credentials')
        response = requests.post(url)

        if response.status_code == 200:
            response_json = response.json()
            return response_json['access_token']
        else:
            response_json = response.json()
            raise PermissionError(f'Error getting client access token from twitch: {response_json["message"]}')

    def get_streams(self) -> dict:
        """Get the twitch streams that currently stream Midair: Community Edition

        :return: A dictionary with the details of the found streams
        """
        if not self.validate_token():
            try:
                self.twitch_access_token = self.get_token()
            except PermissionError as error:
                raise PermissionError(error)
        headers = {
            'Content-type': 'application/json',
            'Authorization': f'Bearer {self.twitch_access_token}',
            'Client-Id': f'{self.twitch_client_id}',
        }
        url = f'https://api.twitch.tv/helix/streams?first=5&game_id={TWITCH_GAME_ID}'
        response = requests.get(url, headers=headers)
        return response.json()

    def validate_token(self) -> bool:
        """Validate the current client access token

        :return: Boolean to indicate if the current token is valid
        """
        headers = {
            'Content-type': 'application/json',
            'Authorization': f'Bearer {self.twitch_access_token}',
            'Client-Id': f'{self.twitch_client_id}',
        }
        url = f'https://id.twitch.tv/oauth2/validate'
        response = requests.get(url, headers=headers)
        return response.status_code == 200
