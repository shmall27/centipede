from utils.github import get_contributors_from_repo, get_github_profile
from utils.db import init_database_if_needed, save_profile_to_db, get_db_connection
from dotenv import load_dotenv
import modal

load_dotenv()

app = modal.App(name="gh-scraper")

# Create an image with our dependencies
image = (modal.Image
         .debian_slim(python_version="3.12")
         .pip_install(
             "pydantic==2.10.4",
             "python-dotenv",
             "duckdb",
         ))


@app.function(
    image=image,
    secrets=[modal.Secret.from_dotenv()],
)
def get_profile_remote(username: str):
    """Wrapper for get_github_profile to run in Modal"""
    try:
        return get_github_profile(username)
    except Exception as e:
        print(f"Error processing {username}: {e}")
        return None


@app.local_entrypoint()
def main():
    init_database_if_needed()
    with get_db_connection() as con:

        # Get list of contributors
        contributors = get_contributors_from_repo("LineageOS", "android")
        usernames = [contributor.login for contributor in contributors]
        print(f"Processing {len(usernames)} contributors...")

        # Get profiles in parallel
        profiles = list(get_profile_remote.map(usernames))

        # Save profiles to database
        for profile in profiles:
            if profile:
                try:
                    save_profile_to_db(con, profile)
                    print(f"Saved profile for: {
                          profile.name if profile.name else 'No name'}")
                except Exception as e:
                    print(f"Error saving profile: {e}")
