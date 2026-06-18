import os

for root, _, files in os.walk('.'):
    if 'node_modules' in root or '.git' in root or '__pycache__' in root or '.venv' in root or '.pytest_cache' in root or '.cecli' in root:
        continue
    for f in files:
        if f.endswith('.txt') or f.endswith('.md') or f.endswith('.py') or f.endswith('.env') or f.endswith('.example') or f.endswith('.sh') or f.endswith('.service') or f.endswith('.timer') or f == 'README.md' or f == '.env.example':
            file_path = os.path.join(root, f)
            try:
                with open(file_path, 'r', encoding='utf-8') as f_in:
                    content = f_in.read()
                    if 'openweather' in content.lower():
                        print(f'Match found in: {file_path}')
            except: pass
