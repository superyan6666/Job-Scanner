import os
import requests
import pandas as pd
import pdfplumber
import tempfile
from urllib.parse import urlparse

# 用户核心专业及相关大类词汇，外加"不限"以防错过无限制好岗
MAJOR_KEYWORDS = ['会计', '工商', '财务', '财经', '经管', '不限']

def parse_attachments(attachments):
    """
    接收附件列表 [{'name': '...', 'url': '...'}, ...]
    下载附件，提取包含专业关键词的表格行，返回格式化后的文本
    """
    matched_lines = []
    
    # 临时目录保存下载的附件
    temp_dir = tempfile.gettempdir()
    
    for att in attachments:
        url = att['url']
        name = att['name']
        
        # 为了安全和兼容，提取文件扩展名
        ext = ''
        if url.lower().endswith('.xls'): ext = '.xls'
        elif url.lower().endswith('.xlsx'): ext = '.xlsx'
        elif url.lower().endswith('.pdf'): ext = '.pdf'
        else: continue
            
        temp_filepath = os.path.join(temp_dir, f"temp_attachment{ext}")
        
        try:
            # 伪装请求头下载
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                with open(temp_filepath, 'wb') as f:
                    f.write(response.content)
            else:
                continue
                
            # 解析 Excel
            if ext in ['.xls', '.xlsx']:
                # engine: .xls 用 xlrd, .xlsx 用 openpyxl (pandas 自动处理，但有时候需要指定)
                try:
                    df = pd.read_excel(temp_filepath)
                    for index, row in df.iterrows():
                        row_str = " | ".join([str(val) for val in row.values if pd.notna(val)])
                        if any(k in row_str for k in MAJOR_KEYWORDS):
                            matched_lines.append(f"【{name}】: {row_str}")
                except Exception as e:
                    print(f"解析 Excel 失败 {name}: {e}")
                    
            # 解析 PDF
            elif ext == '.pdf':
                try:
                    with pdfplumber.open(temp_filepath) as pdf:
                        for page in pdf.pages:
                            tables = page.extract_tables()
                            for table in tables:
                                if not table: continue
                                for row in table:
                                    if not row: continue
                                    row_str = " | ".join([str(cell).replace("\n", "") for cell in row if cell])
                                    if any(k in row_str for k in MAJOR_KEYWORDS):
                                        matched_lines.append(f"【{name}】: {row_str}")
                except Exception as e:
                    print(f"解析 PDF 失败 {name}: {e}")
                    
        except Exception as e:
            print(f"下载附件失败 {url}: {e}")
        finally:
            if os.path.exists(temp_filepath):
                try:
                    os.remove(temp_filepath)
                except:
                    pass
                    
    if not matched_lines:
        return ""
        
    result = "=== 以下为从附件表格中为您专门提取的可能匹配您专业的岗位详情 ===\n"
    for line in matched_lines:
        result += line + "\n"
        
    return result
