'''Replace `!kedro ...` by `kedro ... `'''
import re

filepath = "./demo.html" 
with open(filepath, 'r') as file:
    html_string = "".join(file.readlines())

pattern="""<span class="op">!</span>"""
repl_html_string = re.sub(pattern, repl="", string=html_string)

with open(filepath, 'w') as file:
    file.writelines(repl_html_string)
