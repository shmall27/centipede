from utils.github import get_contributors_from_repo, get_github_profile, read_repos_file
from utils.db import init_database_if_needed, save_profile_to_db, get_db_connection
from utils.search import find_by_language, get_all_users
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
        repos = read_repos_file()
        usernames = get_all_users(con)

        for org, repo in repos:
            print(f"\nProcessing repo: {org}/{repo}")
            try:
                contributors = get_contributors_from_repo(org, repo)
                repo_usernames = [
                    contributor.login for contributor in contributors]
                usernames.update(repo_usernames)
                print(f"Found {len(repo_usernames)} contributors")
            except Exception as e:
                print(f"Error processing {org}/{repo}: {e}")
                continue

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

        # Search for profiles by language
        print("\nSearching for profiles by language...")
        print(find_by_language(con, 'Rust'))
