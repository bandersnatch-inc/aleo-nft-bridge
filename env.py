from dotenv import dotenv_values
from os import path

"""
Loading env variables
"""
BASE_DIR = path.abspath(path.dirname(__file__))

env_environments = ["", "production", "test", "development"]
env_scopes = ["", "local", "global"]

dot_env_dic = {}

for environement in env_environments:
    for scope in env_scopes:
        environement_suffix = f".{environement}" if environement else ""
        scope_suffix = f".{scope}" if scope else ""
        filename = f".env{environement_suffix}{scope_suffix}"
        file_dot_env_dic = dotenv_values(path.join(BASE_DIR, filename))
        dot_env_dic = {**dot_env_dic, **file_dot_env_dic}
        if file_dot_env_dic:
            print("✅ Environement variable file loaded :", filename, "")

for var_name in dot_env_dic:
    glob = globals()
    try:
        glob[var_name] = int(dot_env_dic[var_name])
    except ValueError:
        glob[var_name] = dot_env_dic[var_name]