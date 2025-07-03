from dotenv import load_dotenv
import modal

load_dotenv()

app = modal.App(name="gh-scraper")

image = (modal.Image
         .debian_slim(python_version="3.12")
         .pip_install(
             "pydantic==2.10.4",
             "python-dotenv",
             "duckdb",
             "requests",
         )
         .add_local_python_source("utils"))  # Explicitly add the utils module


@app.function(
    image=image,
    secrets=[modal.Secret.from_dotenv()],
)
def get_profile_remote(username: str):
    """Wrapper for get_github_profile to run in Modal"""
    # Use absolute imports
    from utils.github import get_github_profile

    try:
        return get_github_profile(username)
    except Exception as e:
        print(f"Error processing {username}: {e}")
        return None


@app.function(
    image=image,
    secrets=[modal.Secret.from_dotenv()],
)
def get_contributors_from_repo_remote(target: tuple[str, str]):
    """Wrapper for get_contributors_from_repo to run in Modal"""
    # Use absolute imports
    from utils.github import get_contributors_from_repo

    org, repo = target
    try:
        return get_contributors_from_repo(org, repo)
    except Exception as e:
        print(f"Error processing {org}/{repo}: {e}")
        return []


@app.local_entrypoint()
def main():
    # Use absolute imports for local code
    from utils.github import read_repos_file
    from utils.db import init_database_if_needed, save_profile_to_db, get_db_connection, export_to_csv
    from utils.search import find_by_location, get_all_users

    init_database_if_needed()
    with get_db_connection() as con:
        existing_usernames = get_all_users(con)
        print(f"Found {len(existing_usernames)} existing profiles in database")

        new_usernames = set()
        targets = read_repos_file()

        try:
            contributors = [item for sublist in get_contributors_from_repo_remote.map(
                list(targets)) for item in sublist]
            repo_usernames = {
                contributor.login for contributor in contributors}

            repo_new_usernames = repo_usernames - existing_usernames
            new_usernames.update(repo_new_usernames)
            print(
                f"Found {len(repo_usernames)} contributors ({len(repo_new_usernames)} new)")
        except Exception as e:
            print(f"Error: {e}")

        for location_profile in find_by_location(con, 'CA'):
            print(location_profile)

        if not new_usernames:
            print("\nNo new profiles to fetch!")
            return

        print(f"\nFetching {len(new_usernames)} new profiles...")

        profiles = list(get_profile_remote.map(list(new_usernames)))

        successful = 0
        for profile in profiles:
            if profile:
                try:
                    save_profile_to_db(con, profile)
                    successful += 1
                    print(
                        f"Saved profile for: {profile.name if profile.name else 'No name'}")
                except Exception as e:
                    print(f"Error saving profile: {e}")

        print(
            f"\nSuccessfully saved {successful}/{len(new_usernames)} new profiles")

    export_to_csv(db_path='hiring.db', output_path='hiring.csv')
