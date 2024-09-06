from language_selection import get_top_languages
import time
import json

if __name__ == "__main__":
    langs = get_top_languages(10)
    langs = list(map(lambda x: x[0], langs))

    print("Top 10 languages: ", langs)
    output_data = {
        "date_collected": time.strftime("%Y-%m-%d"),
        "top_10": langs,
    }

    with open("datasets/top_10_languages.json", "w") as f:
        json.dump(output_data, f)
