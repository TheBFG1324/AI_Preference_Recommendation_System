import csv
import random

def generate_user_preferences_csv(filename: str):
    # User attributes
    occupations = ["school teacher", "software engineer", "banker", "doctor", "lawyer", "military member"]
    languages = ["English", "Spanish", "Mandarin", "Arabic", "French"]
    locations = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Diego", "Dallas", "San Jose", "Austin"]
    
    # Opposing preference pairs
    preference_pairs = [
        ("Short responses", "Detailed explanations"),
        ("Bulleted lists", "Fully formed paragraphs"),
        ("Casual and conversational tone", "Formal and academic tone"),
        ("Simple language", "Technical and detailed language"),
        ("High-level overviews", "Context-rich details"),
        ("Frequent examples", "Minimal examples"),
        ("Lighthearted humor", "Strictly serious tone")
    ]
    
    # Standalone preference (no direct opposite)
    standalone_preference = "Use of citations"
    
    # Number of users
    num_users = 2000
    
    with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Header row
        writer.writerow(["user_id", "age", "occupation", "location", "language", "preference_weight", "preference"])
        
        for user_id in range(1, num_users + 1):
            # Random user attributes
            age = random.randint(18, 70)
            occupation = random.choice(occupations)
            location = random.choice(locations)
            language = random.choice(languages)
            
            # Determine how many total preferences we want
            num_preferences = random.randint(4, 7)
            
            # Start by shuffleing the pairs
            shuffled_pairs = preference_pairs[:]
            random.shuffle(shuffled_pairs)
            
            chosen_preferences = []
            
            # Probability of including standalone
            include_standalone = random.random() < 0.3  # 30% chance to include the standalone preference
            slots_for_pairs = num_preferences - (1 if include_standalone else 0)
            
            for pair in shuffled_pairs:
                if len(chosen_preferences) >= slots_for_pairs:
                    break
                choice = random.choice([0, 1, None])  # 0 means first pref, 1 means second pref, None means skip
                if choice is not None:
                    chosen_preferences.append(pair[choice])
            
            if include_standalone and len(chosen_preferences) < num_preferences:
                chosen_preferences.append(standalone_preference)
            
            # Ensure we don't exceed num_preferences
            chosen_preferences = chosen_preferences[:num_preferences]
            
            # Write one line per preference
            for pref in chosen_preferences:
                weight = round(random.random(), 3)  # rounding to 3 decimal places for neatness
                writer.writerow([user_id, age, occupation, location, language, weight, pref])

generate_user_preferences_csv("user_preferences.csv")
