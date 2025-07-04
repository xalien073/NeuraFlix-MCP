import os
import streamlit as st
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from mcp_use import MCPClient, MCPAgent
import asyncio

# Load environment variables
load_dotenv()
os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY")

# Configure Streamlit page
st.set_page_config(page_title="🎬 NeuraFlix Chat Agent", layout="centered")
st.title("🎬 NeuraFlix Chat Agent")
st.markdown("Talk to our AI Movie Agent. Insertt movies, directors, and more!")

# Session state for chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

if "agent" not in st.session_state:
    # Lazy init
    config_file = "neuraflix-mcp.json"
    client = MCPClient.from_config_file(config_file)
    llm = ChatGroq(model="qwen-qwq-32b")

    st.session_state.agent = MCPAgent(
        llm=llm,
        client=client,
        max_steps=15,
        memory_enabled=True,
    )

agent = st.session_state.agent

# Display existing messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Handle user input
if prompt := st.chat_input("Ask me to insert any movie..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        try:
            with st.spinner("Thinking..."):
                response = asyncio.run(agent.run(prompt))
            st.markdown(response)
            st.session_state.messages.append({"role": "assistant", "content": response})
        except Exception as e:
            st.error(f"Error: {e}")

# Clear memory button
if st.button("🧹 Clear Chat & Memory"):
    agent.clear_conversation_history()
    st.session_state.messages = []
    st.rerun()
