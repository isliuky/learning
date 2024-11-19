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

def main():
    st.title("文件修改工具")

    # 文件上传
    uploaded_file = st.file_uploader("上传 .txt 或 .json 文件", type=['txt', 'json'])

    if uploaded_file is not None:
        file_type = uploaded_file.type
        file_content = read_file(uploaded_file)

        if file_content is not None:
            st.write("原始内容:")
            st.write(file_content)

            # 修改内容
            if file_type == 'text/plain':
                modified_content = st.text_area("修改文本内容", value=file_content)
            elif file_type == 'application/json':
                modified_content = st.json(file_content)

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