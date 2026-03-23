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
