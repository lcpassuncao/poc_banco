import streamlit as st
import pandas as pd
import json
import os
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI



def render_chatbot_page():
  genie_space_id = os.getenv("GENIE_SPACE_ID")

  workspace_client = WorkspaceClient(
      host=os.environ.get("DATABRICKS_HOST"),
      client_id=os.environ.get("DATABRICKS_CLIENT_ID"),
      client_secret=os.environ.get("DATABRICKS_CLIENT_SECRET"),
  )
  genie_api = GenieAPI(workspace_client.api_client)
  conversation_id = st.session_state.get("genie_conversation_id", None)

  def ask_genie_sync(question: str, space_id: str, conversation_id: str = None):
      import asyncio
      try:
          loop = asyncio.new_event_loop()
          asyncio.set_event_loop(loop)
          if conversation_id is None:
              initial_message = loop.run_until_complete(
                  loop.run_in_executor(None, genie_api.start_conversation_and_wait, space_id, question)
              )
              conversation_id_local = initial_message.conversation_id
          else:
              initial_message = loop.run_until_complete(
                  loop.run_in_executor(None, genie_api.create_message_and_wait, space_id, conversation_id, question)
              )
              conversation_id_local = conversation_id
          
          answer_json = {"message": ""}
          # Possible text attachment
          for attachment in initial_message.attachments:
              if getattr(attachment, "text", None) and getattr(attachment.text, "content", None):
                  answer_json["message"] = attachment.text.content
                  break
              if getattr(attachment, "query", None):
                  # Attempt to retrieve and display query & results
                  query_result = loop.run_until_complete(
                      loop.run_in_executor(None, genie_api.get_message_query_result,
                          space_id, initial_message.conversation_id, initial_message.id)
                  ) if hasattr(genie_api, "get_message_query_result") else None
                  # Get actual SQL result if present
                  if query_result and hasattr(query_result, "statement_response") and query_result.statement_response:
                      sql_results = loop.run_until_complete(
                          loop.run_in_executor(None, workspace_client.statement_execution.get_statement,
                              query_result.statement_response.statement_id)
                      )
                      answer_json["columns"] = sql_results.manifest.schema.as_dict()
                      answer_json["data"] = sql_results.result.as_dict()
                      desc = getattr(attachment.query, "description", "")
                      answer_json["query_description"] = desc
                      answer_json["sql"] = getattr(attachment.query, "query", "")
                      break
          loop.close()
          return answer_json, conversation_id_local
      except Exception as e:
          return {"message": f"Erro consultando Genie: {str(e)}"}, conversation_id

  def process_query_results(answer_json):
      response_blocks = []
      if "query_description" in answer_json and answer_json["query_description"]:
          response_blocks.append(f"**Descrição da Consulta:** {answer_json['query_description']}")
      if "sql" in answer_json:
          with st.expander("SQL gerado pelo Genie"):
              st.code(answer_json["sql"], language="sql")
      if "columns" in answer_json and "data" in answer_json:
          columns = answer_json["columns"]
          data = answer_json["data"]
          # Safe check for correct structure
          if isinstance(columns, dict) and "columns" in columns:
              # Render Pandas DataFrame in Streamlit
              col_names = [col["name"] for col in columns["columns"]]
              df = pd.DataFrame(data["data_array"], columns=col_names)
              st.markdown("**Resultados da Consulta:**")
              st.dataframe(df)
      elif "message" in answer_json:
          st.markdown(answer_json["message"])
      else:
          st.info("Sem resultados retornados.")
      for block in response_blocks:
          st.markdown(block)

  st.subheader("🤖 Genie: IA para consulta dos dados")
  st.markdown("Pergunte o que quiser sobre o dataset de audiências e obtenha insights em linguagem natural!")

  user_input = st.chat_input("Faça uma pergunta para Genie...")

  if user_input:
      st.chat_message("user").markdown(user_input)
      # Call Genie API and display response
      with st.chat_message("assistant"):
          with st.spinner("Consultando Genie..."):
              answer_json, new_conversation_id = ask_genie_sync(user_input, genie_space_id, conversation_id)
              st.session_state["genie_conversation_id"] = new_conversation_id
              process_query_results(answer_json)





