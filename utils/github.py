import time
from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional, Union
from datetime import datetime
import urllib.request
import urllib.error
import json
import os


class GitHubContributor(BaseModel):
    login: str
    id: int
    node_id: str
    avatar_url: HttpUrl
    gravatar_id: str
    url: HttpUrl
    html_url: HttpUrl
    followers_url: HttpUrl
    following_url: str  # Contains template parameters
    gists_url: str  # Contains template parameters
    starred_url: str  # Contains template parameters
    subscriptions_url: HttpUrl
    organizations_url: HttpUrl
    repos_url: HttpUrl
    events_url: str  # Contains template parameters
    received_events_url: HttpUrl
    type: str
    # Some responses might not include this
    user_view_type: Optional[str] = Field(default="public")
    site_admin: bool
    contributions: int


class GitHubProfile(BaseModel):
    # Enable strict URL parsing but allow None
    class Config:
        strict_url = True

    # Basic Info
    login: str
    name: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[str] = None
    company: Optional[str] = None

    # Social Links - note the Union type for blog
    # Can be None, empty string, or valid URL
    blog: Optional[Union[HttpUrl, str]] = None
    twitter: Optional[str] = Field(None, alias='twitter_username')
    email: Optional[str] = None

    # GitHub Stats
    public_repos: int
    followers: int
    following: int
    created_at: datetime

    # Additional Data
    languages: List[str]
    hireable: Optional[bool] = None

    # Detailed URLs
    html_url: HttpUrl
    repos_url: HttpUrl
    organizations_url: HttpUrl


def read_repos_file() -> list[tuple[str, str]]:
    """
    Read repos.txt file and return list of (org, repo) tuples
    """
    repos = []
    with open('repos.txt', 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                org, repo = line.split('/')
                repos.append((org, repo))
    return repos


def get_github_headers() -> dict:
    """
    Get GitHub API headers with authentication token if available
    Caches the result to avoid reading env variable multiple times
    """
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'Python Script'
    }

    # Get token from environment variable
    token = os.getenv('GITHUB_TOKEN')
    if token:
        headers['Authorization'] = f'Bearer {token}'

    return headers


def check_rate_limits() -> dict:
    """
    Check GitHub API rate limits using /rate_limit endpoint
    Returns dictionary with rate limit information
    """
    url = "https://api.github.com/rate_limit"
    try:
        req = urllib.request.Request(url, headers=get_github_headers())
        with urllib.request.urlopen(req) as response:
            rate_limits = json.loads(response.read().decode('utf-8'))
            # Extract core rate limit info
            core = rate_limits['resources']['core']
            reset_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                       time.localtime(core['reset']))

            print(f"\nGitHub API Rate Limits:")
            print(f"Limit: {core['limit']}")
            print(f"Remaining: {core['remaining']}")
            print(f"Used: {core['used']}")
            print(f"Reset Time: {reset_time}")
            return rate_limits
    except Exception as e:
        print(f"Error checking rate limits: {e}")
        return {}


def make_github_request(url: str) -> dict:
    """Helper function to make authenticated GitHub API requests"""
    req = urllib.request.Request(url, headers=get_github_headers())
    with urllib.request.urlopen(req) as response:
        return json.loads(response.read().decode('utf-8'))


def get_contributors_from_repo(org: str, repo: str) -> list[GitHubContributor]:
    url = f"https://api.github.com/repos/{
        org}/{repo}/contributors?per_page=100"
    try:
        data = make_github_request(url)
        return [GitHubContributor(**contributor) for contributor in data]
    except urllib.error.HTTPError as e:
        print(f"Error accessing API: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return []


def get_github_profile(username: str) -> GitHubProfile:
    """
    Fetch detailed GitHub profile information
    Returns a validated GitHubProfile object
    """
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'Python Script'
    }

    profile = make_github_request(f'https://api.github.com/users/{username}')
    repos = make_github_request(
        f'https://api.github.com/users/{username}/repos')

    # Get user's languages from repos
    languages = set()
    for repo in repos:
        if repo['language']:
            languages.add(repo['language'])

    # Create profile data dict
    profile_data = {
        # Basic Info
        'login': profile.get('login'),
        'name': profile.get('name'),
        'bio': profile.get('bio'),
        'location': profile.get('location'),
        'company': profile.get('company'),

        # Social Links
        'blog': profile.get('blog') or None,  # Convert empty string to None
        'twitter_username': profile.get('twitter_username'),
        'email': profile.get('email'),

        # GitHub Stats
        'public_repos': profile.get('public_repos', 0),
        'followers': profile.get('followers', 0),
        'following': profile.get('following', 0),
        'created_at': profile.get('created_at'),

        # Additional Data
        'languages': list(languages),
        'hireable': profile.get('hireable'),

        # Detailed URLs
        'html_url': profile.get('html_url'),
        'repos_url': profile.get('repos_url'),
        'organizations_url': profile.get('organizations_url')
    }

    return GitHubProfile(**profile_data)
