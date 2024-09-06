from typing import Any
import yaml
import regex as re

languages: dict[str, dict[str, Any]]
filename_index: dict[str, str]
extensions_index: dict[str, str]
interpreters_index: dict[str, str]
heuristics_data: dict[str, Any]

languages = {}
filename_index = {}
extensions_index = {}
interpreters_index = {}
heuristics = []
heuristics_data = {}


def load_language_dataset():
    global languages
    with open("commit_analysis/utils/datasets/linguist/languages.yml", "r") as file:
        languages = yaml.safe_load(file)
    # Build the indexes
    for langName, lang in languages.items():
        if "extensions" in lang:
            for ext in lang["extensions"]:
                extensions_index.setdefault(ext.lower(), [])
                extensions_index[ext.lower()].append(langName)
        if "interpreters" in lang:
            for inter in lang["interpreters"]:
                interpreters_index.setdefault(inter, [])
                interpreters_index[inter].append(langName)
        if "filenames" in lang:
            for filename in lang["filenames"]:
                filename_index.setdefault(filename, [])
                filename_index[filename].append(langName)


def load_heuristics():
    global heuristics_data
    with open("commit_analysis/utils/datasets/linguist/heuristics.yml", "r") as file:
        heuristics_data = yaml.safe_load(file)


load_language_dataset()
load_heuristics()


class Strategy:
    def __call__(
        self, filePath: str, data: bytes, candidate_languages: list[str]
    ) -> list[str]:
        pass


class FilenameStrategy(Strategy):
    def __init__(self):
        pass

    def __call__(
        self, filePath: str, data: bytes, candidate_languages: list[str]
    ) -> list[str]:
        fileName = filePath.split("/")[-1]
        langs = filename_index.get(fileName, [])
        if len(candidate_languages) == 0:
            return langs
        else:
            return [l for l in langs if l in candidate_languages]


class ShebangStrategy(Strategy):
    def __init__(self):
        pass

    def get_interpreter(self, data: bytes):
        # extract shebang from data
        # check if it is a shell shebang
        first_line = data.partition(b"\n")[0].strip()
        if not first_line.startswith(b"#!"):
            return None

        shebang = first_line[2:].strip()
        # Find the interpreter path
        match = re.match(rb"\s*(\S+)", shebang)
        if match:
            interpreter_path = match.group(1)
            interpreter_path = interpreter_path.split(b"/")[
                -1
            ]  # Extract the last part (the interpreter name)

            # If the interpreter is '/usr/bin/env', walk the string to find the actual interpreter
            if interpreter_path == "env":
                arguments = shebang.split()[
                    1:
                ]  # Split the shebang line by spaces, excluding the initial 'env'
                if arguments:
                    interpreter_path = arguments[0]

            # Remove version number from the interpreter (e.g., "python2.6" -> "python2")
            interpreter_path = re.sub(rb"\.\d+$", "", interpreter_path)

            return interpreter_path

        return None

    def __call__(
        self, filePath: str, data: bytes, candidate_languages: list[str]
    ) -> list[str]:
        interpreter = self.get_interpreter(data)
        if interpreter is None:
            return None

        interpreter = interpreter.decode("utf-8")

        langs = interpreters_index.get(interpreter, [])
        if len(langs) == 0:
            return None

        if len(candidate_languages) == 0:
            return langs
        else:
            return [l for l in langs if l in candidate_languages]


class ExtensionStrategy(Strategy):
    def __init__(self):
        pass

    def get_extensions(self, name):
        segments = name.lower().split(".")
        extensions = [
            "." + ".".join(segments[i:]) for i in range(len(segments) - 1, 0, -1)
        ]
        return extensions

    def __call__(
        self, filePath: str, data: bytes, candidate_languages: list[str] = []
    ) -> list[str]:
        fileName = filePath.split("/")[-1]
        extensions = self.get_extensions(fileName)
        possible_langs = []
        for extension in extensions:
            langs_for_ext = extensions_index.get(extension, [])
            if len(candidate_languages) == 0:
                possible_langs.extend(langs_for_ext)
            else:
                for l in langs_for_ext:
                    if l in candidate_languages:
                        possible_langs.append(l)
        return possible_langs


class Heuristic:
    def __init__(self, exts, rules):
        self.exts = exts
        self.rules = rules

    def extensions(self):
        return self.exts

    def languages(self):
        return list(set([rule["language"] for rule in self.rules]))

    def matches(self, filename: str, candidates: list[str]):
        filename = filename.lower()
        candidates = [c.lower() for c in candidates if c is not None]
        return any(filename.endswith(ext) for ext in self.exts)

    def call(self, data: bytes):
        matched = next(
            (rule for rule in self.rules if rule["pattern"].search(data)),
            None,
        )
        if matched:
            languages = matched["language"]
            return (
                [name for name in languages]
                if isinstance(languages, list)
                else [languages]
            )


class HeuristicStrategy(Strategy):
    HEURISTICS_CONSIDER_BYTES = 50 * 1024

    class And:
        def __init__(self, patterns):
            self.patterns = patterns

        def search(self, input):
            res = [pat.search(input) for pat in self.patterns]
            return all(res)

    class AlwaysMatch:
        def search(self, input):
            return True

    class NeverMatch:
        def search(self, input):
            return False

    class NegativePattern:
        def __init__(self, pat):
            self.pat = pat

        def search(self, input):
            return not self.pat.search(input)

    heuristics: list[Heuristic]

    def __init__(self):
        self.heuristics = []
        self.named_patterns = {
            k: self.to_regex(v) for k, v in heuristics_data["named_patterns"].items()
        }

        for disambiguation in heuristics_data["disambiguations"]:
            exts = disambiguation["extensions"]
            rules = disambiguation["rules"]
            rules = [
                {
                    "pattern": self.parse_rule(self.named_patterns, rule),
                    "language": rule["language"],
                }
                for rule in rules
            ]
            self.heuristics.append(Heuristic(exts, rules))
        pass

    @classmethod
    def to_regex(cls, s: str):
        if isinstance(s, list):
            return re.compile(bytes("|".join(s), encoding="utf-8"), re.MULTILINE)
        else:
            return re.compile(bytes(s, encoding="utf-8"), re.MULTILINE)

    @classmethod
    def parse_rule(self, named_patterns, rule):
        try:
            if "and" in rule:
                rules = [
                    self.parse_rule(named_patterns, block) for block in rule["and"]
                ]
                return self.And(rules)
            elif "pattern" in rule:
                return self.to_regex(rule["pattern"])
            elif "negative_pattern" in rule:
                pat = self.to_regex(rule["negative_pattern"])
                return self.NegativePattern(pat)
            elif "named_pattern" in rule:
                return named_patterns[rule["named_pattern"]]
            else:
                return self.AlwaysMatch()
        except Exception as e:
            print("Error parsing rule (" + str(rule) + "), disabling: " + str(e))
            return self.NeverMatch()

    def __call__(
        self, filePath: str, data: bytes, candidate_languages: list[str]
    ) -> list[str]:
        data = data[: self.HEURISTICS_CONSIDER_BYTES]
        fileName = filePath.split("/")[-1]
        new_candidate_languages = candidate_languages.copy()
        for heuristic in self.heuristics:
            if heuristic.matches(fileName, candidate_languages):
                heur_candid_langs = heuristic.call(data)
                if heur_candid_langs is None:
                    for lang in heuristic.languages():
                        if lang in new_candidate_languages:
                            new_candidate_languages.remove(lang)
                elif len(heur_candid_langs) == 1:
                    return heur_candid_langs

        return new_candidate_languages


class LanguageClassifier:
    STRATEGIES = [
        FilenameStrategy(),
        ShebangStrategy(),
        ExtensionStrategy(),
        HeuristicStrategy(),
    ]

    def get_lang_by_blob(self, file_name: str, data: bytes):
        candidate_languages = []
        for strategy in self.STRATEGIES:
            langs = strategy(file_name, data, candidate_languages)
            # print(langs)
            if langs is None:
                continue
            if len(langs) == 1:
                return langs[0]
            candidate_languages = langs

        return ",".join(candidate_languages)

    PATH_STRATEGIES = [
        FilenameStrategy(),
        ExtensionStrategy(),
    ]

    def get_lang_by_path(self, file_name: str):
        candidate_languages = []
        for strategy in self.PATH_STRATEGIES:
            langs = strategy(file_name, None, candidate_languages)
            if langs is None:
                continue
            if len(langs) == 1:
                return langs[0]
            candidate_languages = langs

        return "|".join(candidate_languages)
