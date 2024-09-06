import regex
from pyrepositoryminer.pobjects import Blob, Object
from repository_analysis.utils import LanguageClassifier
from pygit2 import Blob as pBlob
import time

test_perl = """### Usermods

This folder serves as a repository for usermods (custom `usermod.cpp` files)!

If you have created a usermod you believe is useful (for example to support a particular sensor, display, feature...), feel free to contribute by opening a pull request!

In order for other people to be able to have fun with your usermod, please keep these points in mind:

-   Create a folder in this folder with a descriptive name (for example `usermod_ds18b20_temp_sensor_mqtt`)  
-   Include your custom files 
-   If your usermod requires changes to other WLED files, please write a `readme.md` outlining the steps one needs to take  
-   Create a pull request!  
-   If your feature is useful for the majority of WLED users, I will consider adding it to the base code!  

While I do my best to not break too much, keep in mind that as WLED is updated, usermods might break.  
I am not actively maintaining any usermod in this directory, that is your responsibility as the creator of the usermod.

For new usermods, I would recommend trying out the new v2 usermod API, which allows installing multiple usermods at once and new functions!
You can take a look at `EXAMPLE_v2` for some documentation and at `Temperature` for a completed v2 usermod!

Thank you for your help :)"""

lang_classifier = LanguageClassifier()

# perl_rule = "|".join(perl_matches)
# perl_rule = regex.compile(perl_rule)
# nreg = "^\s*use\s+v6\b"

fileName = "usermods/readme.md"
data = test_perl.encode("utf-8")

start_time = time.time()

res = lang_classifier.get_lang_by_blob(file_name=fileName, data=data)

end_time = time.time()
print(f"Execution time: {end_time - start_time} seconds")

print(res)
# print(perl_rule)
# print(regex.search(perl_match, test_perl, regex.MULTILINE))


# print(lang_classifier.use_shebang(data=data))
