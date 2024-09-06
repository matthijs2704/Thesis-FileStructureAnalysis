from collections import defaultdict
import pandas as pd
import numpy as np
import regex as re
import matplotlib.pyplot as plt
import scipy.stats as stats

languages = [
    "Python",
    "Javascript",
    "Java",
    "C#",
    ["C", "C++"],
    "Typescript",
    "PHP",
    "Rust",
    "Swift",
    "Go",
]

tutorial_stat = {}
size_stats = {}
stars_stats = {}
total_size_stats = {}


# exit(0)

df = pd.read_csv(f"output/github_data_filtered.csv", keep_default_na=False)

#### Filter stats ####
project_ignored = df["ignored"]
ignored_projects = df[project_ignored]
ignored_per_pc = ignored_projects.groupby(["lang"]).size()
total_per_pc = df.groupby(["lang"]).size()

print("Ignored projects per language:")
ignored_per_pc = df[df["ignored"]].groupby(["lang"]).size()
total_per_pc = df.groupby(["lang"]).size()
ignored_percent_per_pc = round(ignored_per_pc / total_per_pc * 100, 2)
filter_stats = pd.DataFrame(
    {
        "Filtered Projects": ignored_per_pc,
        "Total Projects": total_per_pc,
        "Remaining Projects": total_per_pc - ignored_per_pc,
        "Ignored Projects (%)": ignored_percent_per_pc,
    }
)
filter_stats["Filtered Projects"] = filter_stats["Filtered Projects"].astype(int)
filter_stats["Total Projects"] = filter_stats["Total Projects"].astype(int)
filter_stats["Remaining Projects"] = filter_stats["Remaining Projects"].astype(int)
print(filter_stats.to_string())

ignored_projects.to_csv("output/ignored_projects.csv", index=False)

df = df[~project_ignored]  # Filter all the ignored repos, for the rest of the analysis

# mean_size = df["size"].mean()
# std_size = df["size"].std()
# size_upper = mean_size + 4 * std_size
# print(f"Mean size: {mean_size}")
# print(f"Std size: {std_size}")
# print(f"Upper size: {size_upper}")
# print(f"Number of projects: {len(df)}")

# QQ plot for size

projects_bigger_than_10g = df[df["size"] > 10 * 1_048_576]
print(f"Number of projects >10gb: {len(projects_bigger_than_10g)}")
print(
    projects_bigger_than_10g.sort_values(by="size", ascending=False)[
        ["owner.name", "name", "description", "topics", "size", "ignored"]
    ]
)
plt.hist(df["size"] / 1_048_576, bins=200)
plt.axvline(10, color="r", linestyle="--")
plt.ylim(0, 50)
plt.xlabel("Size (GB)")
plt.ylabel("Number of projects")
plt.title("Size of projects")
plt.savefig("output/project_size_plot.pdf")
# plt.show()

print("Size statistics per PL:")
size_per_pc_desc = df.groupby(["lang"])["size"].describe()
print(size_per_pc_desc.to_string())

print("Star statistics per PL:")
stars_per_pc_desc = df.groupby(["lang"])["stargazers_count"].describe()
for col in ["count", "min", "max", "25%", "50%", "75%", "max"]:
    stars_per_pc_desc[col] = stars_per_pc_desc[col].astype(int)
print(stars_per_pc_desc.to_string())
exit(0)

# Get highest 0.1% of sizes
size_upper = df["size"].quantile(0.999)
print(f"Size upper: {size_upper}")
big_projects = df[df["size"] > size_upper]
print(f"Number of big projects: {len(big_projects)}")
# Show 3 largest projects
print(big_projects.sort_values(by="size", ascending=False)[["name", "size"]].head(3))

# Remove empty projects
# empty_proj = df[df["size"] < 100]
# print(f"Num empty projects for {lang}: {len(empty_proj)}")
# print(empty_proj[["name", "url", "size"]].head(2).to_string())
stars_stats[language_name] = df["stargazers_count"].describe()
df = df.drop(columns=["ignored_name", "ignored_topic", "ignored_description"])

exit(0)

for lang in languages:
    #     language_name = "_".join(lang) if isinstance(lang, list) else lang
    # Load dataset for Python
    df = pd.read_csv(f"output/github_data/filtered/repos_{language_name}.csv")
    df = df[~df["ignored"]]  # Filter all the ignored repos

    # df["size"] = df["size"] / 1024
    size_stats[language_name] = df["size"].describe()
    total_size_stats[language_name] = df["size"].sum()

    # Get highest 0.1% of sizes
    size_upper = df["size"].quantile(0.999)
    print(f"Size upper: {size_upper}")
    big_projects = df[df["size"] > size_upper]
    print(f"Number of big projects: {len(big_projects)}")
    # Show 3 largest projects
    print(
        big_projects.sort_values(by="size", ascending=False)[["name", "size"]].head(3)
    )

    # Remove empty projects
    # empty_proj = df[df["size"] < 100]
    # print(f"Num empty projects for {lang}: {len(empty_proj)}")
    # print(empty_proj[["name", "url", "size"]].head(2).to_string())

    stars_stats[language_name] = df["stargazers_count"].describe()

    df = df.drop(columns=["ignored_name", "ignored_topic", "ignored_description"])

    # # List of distinct topics
    # topics = df["descr_topics"].values.flatten().tolist()
    # # print(topics)
    # for projTopics in topics:
    #     if not projTopics:
    #         continue

    #     for topic in projTopics:
    #         pc_topics[topic] += 1


print("Size stats:")
print(pd.DataFrame(size_stats).to_string())

print("Stars stats:")
print(pd.DataFrame(stars_stats).to_string())

print("Total size stats:")
print((pd.Series(total_size_stats) / 1048576).to_string())
