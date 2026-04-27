import os
import ast
from pathlib import Path

# --- 설정 ---
ROOT_DIR = "src/asset_portfolio"
OUTPUT_PATH = "docs/Project_structure.md"
EXCLUDE_DIRS = {"__pycache__", ".venv", ".git", ".pytest_cache", ".streamlit"}

def get_project_tree(root_dir, indent=""):
    """프로젝트 폴더 및 파일 구조를 트리 형태로 생성"""
    lines = []
    items = sorted(os.listdir(root_dir))
    
    for i, item in enumerate(items):
        if item in EXCLUDE_DIRS:
            continue
            
        path = os.path.join(root_dir, item)
        is_last = (i == len(items) - 1)
        prefix = "└── " if is_last else "├── "
        
        lines.append(f"{indent}{prefix}{item}")
        
        if os.path.isdir(path):
            extension = "    " if is_last else "│   "
            lines.extend(get_project_tree(path, indent + extension))
            
    return lines

def extract_functions_from_file(file_path):
    """Python 파일에서 클래스와 함수 목록 및 Docstring 추출"""
    funcs = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            node = ast.parse(f.read())
            
        for item in node.body:
            # 최상위 함수 정의
            if isinstance(item, ast.FunctionDef):
                doc = ast.get_docstring(item)
                desc = f": {doc.splitlines()[0]}" if doc else ""
                funcs.append(f"- `def {item.name}()`{desc}")
                
            # 클래스 및 클래스 내부 메서드 정의
            elif isinstance(item, ast.ClassDef):
                doc = ast.get_docstring(item)
                desc = f": {doc.splitlines()[0]}" if doc else ""
                funcs.append(f"- `class {item.name}`{desc}")
                for subitem in item.body:
                    if isinstance(subitem, ast.FunctionDef):
                        sub_doc = ast.get_docstring(subitem)
                        sub_desc = f": {sub_doc.splitlines()[0]}" if sub_doc else ""
                        funcs.append(f"  - `def {subitem.name}()`{sub_desc}")
    except Exception as e:
        return [f"- (Error parsing file: {e})"]
    
    return funcs

def main():
    # 저장 디렉토리 생성
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    content = []
    content.append("# 📂 Project Structure & Function List")
    content.append(f"\nLast Updated: {os.popen('date /t' if os.name == 'nt' else 'date').read().strip()}\n")
    
    # 1. 프로젝트 구조 도출
    content.append("## 1. Directory Tree")
    content.append("```")
    content.append("src/asset_portfolio/")
    content.extend(get_project_tree(ROOT_DIR))
    content.append("```\n")
    
    # 2. 함수 리스트 도출
    content.append("## 2. Function List by File")
    
    for root, dirs, files in os.walk(ROOT_DIR):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        for file in sorted(files):
            if file.endswith(".py"):
                full_path = Path(root) / file
                relative_path = full_path.relative_to(os.getcwd())
                
                content.append(f"\n### `{relative_path}`")
                funcs = extract_functions_from_file(full_path)
                if funcs:
                    content.extend(funcs)
                else:
                    content.append("- (No functions defined)")
                    
    # 파일 쓰기
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(content))
        
    print(f"✅ Documentation updated: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()