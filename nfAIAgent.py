"""CLIAgent"""
import asyncio
import os
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from mcp_use import MCPClient, MCPAgent

async def run_memory_chat():
    """Run a chat using MCPAgent's built-in conversation memory."""
    # Load environment variables
    load_dotenv()
    os.environ["GROQ_API_KEY"] = os.getenv("GROQ_API_KEY")

    config_file = "neuraflix-mcp.json"

    print("Initializing Movie Agent Chat...")

    # Create MCP client and agent
    client = MCPClient.from_config_file(config_file)
    llm = ChatGroq(model="qwen/qwen3-32b")

    agent = MCPAgent(
        llm=llm,
        client=client,
        max_steps=15,
        memory_enabled=True,
    )

    print("\n===== Talk to your Neuraflix Agent =====")
    print("Type 'exit' or 'quit' to end the chat")
    print("Type 'clear' to reset memory")
    print("==========================================\n")

    try:
        while True:
            user_input = input("\nYou: ")

            if user_input.lower() in ["exit", "quit"]:
                print("Ending conversation...")
                break

            if user_input.lower() == "clear":
                agent.clear_conversation_history()
                print("Conversation history cleared.")
                continue

            print("\nNF-MCP-Agent: ", end="\n", flush=True)
            try:
                response = await agent.run(user_input)
                print(response)
            except Exception as e:
                print(f"\nError: {e}")

    finally:
        if client and client.sessions:
            print("\nClosing all client sessions...")
            await client.close_all_sessions()


if __name__ == "__main__":
    asyncio.run(run_memory_chat())
