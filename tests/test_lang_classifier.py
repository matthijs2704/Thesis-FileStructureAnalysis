from repository_analysis.utils import LanguageClassifier

lang_classifier = LanguageClassifier()
lang = lang_classifier.get_lang_by_path("Makefile.txt")
print(lang)
