import re

def normalize_domain(url):
    """Clean the URL and extract the normalized domain."""
    if not url:
        return ""
    
    url = url.lower().strip()
    
    # Remove http:// or https://
    url = re.sub(r'^https?://', '', url)
    
    # Remove www.
    url = re.sub(r'^www\.', '', url)
    
    # Split by / to get only the domain
    domain = url.split('/')[0]
    
    return domain
