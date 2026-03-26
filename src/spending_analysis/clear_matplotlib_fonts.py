import matplotlib as mpl
import shutil
import os

cache_dir = mpl.get_cachedir()
print(f"Matplotlib cache directory: {cache_dir}")

# Remove the entire cache directory
if os.path.exists(cache_dir):
    try:
        shutil.rmtree(cache_dir)
        print("Matplotlib font cache cleared successfully.")
    except Exception as e:
        print(f"Error clearing cache: {e}")
else:
    print("Matplotlib cache directory not found. It might be created on next run.")

# You might also want to clear any fontconfig cache if it exists (less common on macOS for this specific issue, but good to know)
# On macOS, fontconfig cache is usually in ~/.cache/fontconfig
# if os.path.exists(os.path.expanduser('~/.cache/fontconfig')):
#     try:
#         shutil.rmtree(os.path.expanduser('~/.cache/fontconfig'))
#         print("Fontconfig cache cleared successfully.")
#     except Exception as e:
#         print(f"Error clearing fontconfig cache: {e}")
