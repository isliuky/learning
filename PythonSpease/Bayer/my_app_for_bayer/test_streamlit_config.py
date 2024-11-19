import streamlit as st
import json

def read_file(file):
    if file.type == 'text/plain':
        return file.read().decode('utf-8')
    elif file.type == 'application/json':
        return json.loads(file.read().decode('utf-8'))
    else:
        return None

def write_file(data, file_type):
    if file_type == 'text/plain':
        return data.encode('utf-8')
    elif file_type == 'application/json':
        return json.dumps(data, indent=4).encode('utf-8')
    else:
        return None

def replace_text(content, replacements):
    if isinstance(content, str):
        for old, new in replacements.items():
            content = content.replace(old, new)
    elif isinstance(content, dict):
        for key, value in content.items():
            if isinstance(value, str):
                for old, new in replacements.items():
                    content[key] = content[key].replace(old, new)
            elif isinstance(value, dict):
                replace_text(value, replacements)
    return content

def main():
    st.title("文件批量替换工具")

    # 文件上传
    uploaded_file = st.file_uploader("上传 .txt 或 .json 文件", type=['txt', 'json'])

    if uploaded_file is not None:
        file_type = uploaded_file.type
        file_content = read_file(uploaded_file)

        if file_content is not None:
            st.write("原始内容:")
            if file_type == 'text/plain':
                st.code(file_content)
            elif file_type == 'application/json':
                st.json(file_content)

            # 定义替换规则
            replacements = {
                '268754486553': '828533277754',
                '850255527329': '828533277754',
                'ph-cdp-prod-cn-north-1': 'ph-cdp-nprod-qa-cn-north-1',
                'ph-cdp-nprod-dev-cn-north-1': 'ph-cdp-nprod-qa-cn-north-1',
                'cn-north-1-prod': 'cn-north-1-qa',
                'cn-north-1-dev': 'cn-north-1-qa',
                'ph-cdp-sftp-outbound-prod': 'ph-cdp-sftp-outbound-qa',
                'ph-cdp-sftp-outbound-dev': 'ph-cdp-sftp-outbound-qa',
                'ph-cdp-sftp-inbound-prod': 'ph-cdp-sftp-inbound-qa',
                'ph-cdp-sftp-inbound-dev': 'ph-cdp-sftp-inbound-qa',
                'From_prod': 'From_qa',
                'From_dev': 'From_qa',
                'cn_cdp_prod': 'cn_cdp_qa',
                'cn_cdp_dev': 'cn_cdp_qa',
                '-dev-cn-north-1': '-qa-cn-north-1',
                '-prod-cn-north-1': '-qa-cn-north-1',
                'phcdp/mssql/fusion-p-1': 'phcdp/mssql/fusion-q',
                'phcdp/mssql/mao-p': 'phcdp/mssql/mao-q'
            }

            # 执行替换
            modified_content = replace_text(file_content, replacements)

            st.write("修改后的内容:")
            if file_type == 'text/plain':
                st.code(modified_content)
            elif file_type == 'application/json':
                st.json(modified_content)

            if st.button("保存并下载"):
                new_file_content = write_file(modified_content, file_type)
                st.download_button(
                    label="下载修改后的文件",
                    data=new_file_content,
                    file_name=uploaded_file.name,
                    mime=file_type
                )

if __name__ == "__main__":
    main()