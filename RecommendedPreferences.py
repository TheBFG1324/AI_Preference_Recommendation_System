# Import Libraries
from neo4j import GraphDatabase
import csv
from collections import Counter
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Fetch Neo4j connection details and file paths from environment variables
URI = os.getenv("NEO4J_URI")
USERNAME = os.getenv("NEO4J_USERNAME")
PASSWORD = os.getenv("NEO4J_PASSWORD")
CSV_FILE = os.getenv("CSV_FILE")

class Neo4jHandler:
    def __init__(self, uri, user, password):
        # Initialize the Neo4j driver with given URI and credentials
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        # Close the database connection
        self.driver.close()

    def run_query(self, query, parameters=None):
        # Run a Cypher query on the Neo4j database with optional parameters
        with self.driver.session() as session:
            return list(session.run(query, parameters))

    def create_constraints(self):
        # Create constraints on User and Preference nodes to ensure uniqueness
        queries = [
            "CREATE CONSTRAINT IF NOT EXISTS ON (u:User) ASSERT u.user_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS ON (p:Preference) ASSERT p.name IS UNIQUE"
        ]
        for q in queries:
            self.run_query(q)

    def load_data(self, csv_file):
        # Clear existing data from the database
        self.run_query("MATCH (n) DETACH DELETE n")

        # Create constraints on the database
        self.create_constraints()

        # Read user data from the CSV file
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            user_preferences = {}
            user_attributes = {}

            # Iterate through each row in the CSV
            for row in reader:
                user_id = int(row["user_id"])
                age = int(row["age"])
                occupation = row["occupation"]
                location = row["location"]
                language = row["language"]
                weight = float(row["preference_weight"])
                preference = row["preference"]

                # If this is the first time we see this user_id, store their attributes
                if user_id not in user_attributes:
                    user_attributes[user_id] = {
                        "user_id": user_id,
                        "age": age,
                        "occupation": occupation,
                        "location": location,
                        "language": language
                    }

                # Append the preference and its weight to this user's preference list
                user_preferences.setdefault(user_id, []).append((preference, weight))

            # Insert users and preferences into the database
            with self.driver.session() as session:
                # Create User nodes
                for uid, attrs in user_attributes.items():
                    session.run(
                        "CREATE (u:User {user_id: $user_id, age: $age, occupation: $occupation, location: $location, language: $language})",
                        attrs
                    )
                
                # Collect all unique preferences
                all_prefs = set()
                for prefs in user_preferences.values():
                    for p, w in prefs:
                        all_prefs.add(p)

                # Ensure each preference exists as a node in the database
                for p in all_prefs:
                    session.run("MERGE (pref:Preference {name: $name})", {"name": p})

                # Create relationships from User to Preference with a weight property
                for uid, prefs in user_preferences.items():
                    for p, w in prefs:
                        session.run("""
                            MATCH (u:User {user_id: $uid})
                            MATCH (pref:Preference {name: $p})
                            MERGE (u)-[r:HAS_PREFERENCE {weight: $w}]->(pref)
                        """, {"uid": uid, "p": p, "w": w})

    def get_preferences_of_users(self, user_ids):
        # Given a list of user_ids, return their preferences
        # If no users are provided, return an empty dictionary
        if not user_ids:
            return {}
        
        # Fetch user preferences for all given user_ids
        query = """
        MATCH (u:User)-[:HAS_PREFERENCE]->(p:Preference)
        WHERE u.user_id IN $user_ids
        RETURN u.user_id AS uid, p.name AS pref
        """
        result = self.run_query(query, {"user_ids": user_ids})
        prefs_by_user = {}

        # Organize preferences by user_id
        for record in result:
            uid = record["uid"]
            pref = record["pref"]
            prefs_by_user.setdefault(uid, []).append(pref)

        return prefs_by_user

    def get_similar_users_by_attributes(self, age, occupation, location, language):
        # Define an age range of ±10 years around the given age
        min_age = age - 10
        max_age = age + 10

        # Find up to 5 users that match the given occupation, location, language and fall within the age range
        query = """
        MATCH (u:User)
        WHERE u.occupation = $occupation
        AND u.location = $location
        AND u.language = $language
        AND u.age >= $min_age
        AND u.age <= $max_age
        RETURN u.user_id AS uid
        LIMIT 5
        """

        result = self.run_query(query, {
            "occupation": occupation,
            "location": location,
            "language": language,
            "min_age": min_age,
            "max_age": max_age
        })

        # Return a list of similar user IDs
        return [record["uid"] for record in result]

    def recommend_preferences_for_new_user(self, age, occupation, location, language):
        # Find similar users for a "new" user who is not in the database
        similar_users = self.get_similar_users_by_attributes(age, occupation, location, language)

        # If no similar users found, return an empty list
        if not similar_users:
            return []

        # Retrieve preferences of these similar users
        prefs_by_user = self.get_preferences_of_users(similar_users)

        # Count how often each preference appears among these similar users
        counter = Counter()
        for uid, prefs in prefs_by_user.items():
            for p in prefs:
                counter[p] += 1

        # Return the top 5 most common preferences
        recommendations = counter.most_common(5)
        return recommendations


if __name__ == "__main__":
    # Initialize the Neo4j handler with the given credentials
    handler = Neo4jHandler(URI, USERNAME, PASSWORD)
    
    # Load CSV data into the Neo4j database
    handler.load_data(CSV_FILE)
    
    # Example: attributes of a new user not currently in the database
    new_user_age = 30
    new_user_occupation = "software engineer"
    new_user_location = "New York"
    new_user_language = "English"

    # Get recommended preferences for this "new" user based on similar existing users
    recommendations = handler.recommend_preferences_for_new_user(new_user_age, new_user_occupation, new_user_location, new_user_language)
    print("Recommended preferences for the new user: ", recommendations)

    # Close the database connection
    handler.close()
