import pandas as pd

def calculate_ig_collab(df: pd.DataFrame) -> pd.Series:
    """Calculates if an Instagram post is a collaboration."""
    if 'coauthorProducers/0/username' in df.columns:
        return df['coauthorProducers/0/username'].notna()
    elif 'taggedUsers/0/username' in df.columns:
        return df['taggedUsers/0/username'].notna()
    return pd.Series(False, index=df.index)


PLATFORM_MAPPINGS = {
    "Instagram (Apify)": {
        "actor_id": "apify/instagram-scraper",
        "base_platform": "instagram",
        "columns": {
            'ownerUsername': 'username',
            'videoPlayCount': 'views',
            'likesCount': 'likes',
            'commentsCount': 'comments',
            'audioUrl': 'audio_url',
            'timestamp': 'published_date',
        },
        "custom_transforms": {
            'is_collab': calculate_ig_collab
        }
    },

    "TikTok (Apify)": {
        "actor_id": "clockwork/tiktok-profile-scraper",
        "base_platform": "tiktok",
        "columns": {
            'authorMeta/name': 'username',
            'playCount': 'views',
            'diggCount': 'likes',
            'commentCount': 'comments',
            'createTimeISO': 'published_date'
        },
        "custom_transforms": {
            # TikTok doesn't have complex collab logic yet, just force it to False
            'is_collab': lambda df: pd.Series(False, index=df.index)
        }
    },

    "YouTube (Apify)": {
        'channelName': 'username',
        'viewCount': 'views',
        'likeCount': 'likes',
        'commentCount': 'comments',
        'date': 'published_date'
    }
}


def get_available_platforms() -> list[str]:
    """Returns a list of supported platforms for the UI dropdown."""
    return list(PLATFORM_MAPPINGS.keys())


_PROFILE_URL_TEMPLATES: dict[str, str] = {
    'instagram': "https://www.instagram.com/{}/",
    'tiktok': "https://www.tiktok.com/@{}",
    'youtube': "https://www.youtube.com/{}",
}


def build_profile_url(platform: str, username: str) -> str:
    """Builds the canonical profile URL for a given platform."""
    template = _PROFILE_URL_TEMPLATES.get(platform)
    if template is None:
        raise ValueError(f"Unsupported platform for URL generation: {platform}")
    return template.format(username)
