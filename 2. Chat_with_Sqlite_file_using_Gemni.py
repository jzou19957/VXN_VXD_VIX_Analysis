import os
import glob
import sqlite3
import logging
import traceback
import subprocess
import sys
import pandas as pd
import numpy as np
import re
import json
import uuid
import importlib
from typing import Dict, List, Any, Tuple
import google.generativeai as generativeai
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("data_analysis.log"), logging.StreamHandler()])
logger = logging.getLogger(__name__)

# Configure Google Gemini API
API_KEY = "###"  # Replace with your own free API at https://ai.google.dev/
generativeai.configure(api_key=API_KEY)
model = generativeai.GenerativeModel('gemini-1.5-flash')

def print_and_log(message: str):
    """Utility function to print and log messages."""
    print(message)
    logger.info(message)

def call_generative_api(prompt: str) -> str:
    """Function to call the Google Gemini API with a prompt."""
    print_and_log("Sending request to Generative API...")
    try:
        response = model.generate_content(prompt)
        print_and_log("Received response from Generative API.")
        return response.text
    except Exception as exception:
        print_and_log(f"Error calling Generative API: {str(exception)}")
        return None

def extract_code_blocks(llm_response: str) -> List[str]:
    """Extracts multiple code blocks from the LLM response."""
    code_blocks = re.findall(r"#begin(.*?)#end", llm_response, re.DOTALL)
    if not code_blocks:
        raise ValueError("No valid code blocks found in the LLM response.")
    return [block.strip() for block in code_blocks]

def clean_python_code(code: str) -> str:
    """Cleans the generated Python code to ensure it is valid."""
    # Remove any markdown-like syntax
    code = re.sub(r'-\s*\*\*.*?\*\*\s*:', '', code)
    return code

def install_package(package_name: str, retries: int = 3):
    """Installs a package using pip with retries."""
    for attempt in range(retries):
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
            return  # Successfully installed, exit function
        except subprocess.CalledProcessError as e:
            print_and_log(f"Failed to install {package_name} on attempt {attempt + 1}: {e}")
    raise RuntimeError(f"Failed to install {package_name} after {retries} attempts.")

def execute_python_code(code_blocks: List[str], output_folder: str) -> str:
    """Executes the provided Python code blocks sequentially and returns the output."""
    output_str = ""
    for idx, code in enumerate(code_blocks):
        cleaned_code = clean_python_code(code)
        code_filename = f"analysis_code_{uuid.uuid4().hex[:8]}.py"
        
        with open(code_filename, "w") as f:
            f.write(cleaned_code)

        try:
            # Check and install missing packages
            required_packages = re.findall(r"import (\w+)|from (\w+) import", cleaned_code)
            for package in required_packages:
                package_name = package[0] or package[1]
                try:
                    importlib.import_module(package_name)
                except ImportError:
                    try:
                        install_package(package_name)
                    except Exception as e:
                        print_and_log(f"Failed to install {package_name}. Trying fallback libraries.")
                        # Implement fallback logic here
                        # For example, if matplotlib fails, try plotly
                        if package_name == "matplotlib":
                            try:
                                install_package("plotly")
                                cleaned_code = cleaned_code.replace("matplotlib", "plotly")
                            except Exception as e:
                                print_and_log(f"Fallback to plotly failed: {e}")
                                raise

            # Execute the code block
            result = subprocess.run([sys.executable, code_filename], capture_output=True, text=True, check=True)
            output_path = os.path.join(output_folder, f"{code_filename.replace('.py', f'_output_{idx}.txt')}")
            with open(output_path, "w") as output_file:
                output_file.write(result.stdout)
            print_and_log(f"Python code block {idx + 1} executed successfully. Output saved to {output_path}")
            output_str += result.stdout + "\n"
        except subprocess.CalledProcessError as e:
            error_message = f"Error executing Python code block {idx + 1}: {e.stderr}\n{traceback.format_exc()}"
            print_and_log(error_message)
            raise RuntimeError(error_message)
        finally:
            os.remove(code_filename)  # Clean up generated script file
    return output_str.strip()

class DataAnalysisTool:
    def __init__(self):
        self.db_file = self.find_db_file()
        self.db_structure = self.get_db_structure()
        self.sample_data = self.get_sample_data()
        self.output_folder = "analysis_results"
        os.makedirs(self.output_folder, exist_ok=True)

    def find_db_file(self) -> str:
        """Finds the first .db file in the current directory."""
        db_files = glob.glob("*.db")
        if not db_files:
            raise FileNotFoundError("No .db file found in the current directory.")
        return db_files[0]

    def get_db_structure(self) -> Dict[str, List[str]]:
        """Retrieves the structure of the SQLite database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        structure = {}
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            structure[table_name] = [col[1] for col in columns]
        conn.close()
        return structure

    def get_sample_data(self, sample_size: int = 5) -> Dict[str, List[Dict[str, Any]]]:
        """Retrieves sample data from each table in the database."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        sample_data = {}
        for table_name, columns in self.db_structure.items():
            cursor.execute(f"SELECT * FROM {table_name} LIMIT {sample_size}")
            rows = cursor.fetchall()
            sample_data[table_name] = [dict(zip(columns, row)) for row in rows]
        conn.close()
        return sample_data

    def assess_query_complexity(self, user_query: str) -> Tuple[str, str]:
        """Assesses the complexity of the user's query."""
        prompt = f"""
        Analyze the following user query and database structure to determine the most appropriate method to answer it:
        User Query: "{user_query}"
        Database Structure: {json.dumps(self.db_structure, indent=2)}
        Sample Data: {json.dumps(self.sample_data, indent=2)}
        Determine which of the following methods is most appropriate:
        1. "SIMPLE_SQLITE" - If the query can be answered with a straightforward SQLite query.
        2. "COMPLEX_SQLITE" - If the query requires a more complex SQLite query (e.g., multiple joins, subqueries).
        3. "PYTHON_PANDAS" - If the query requires data manipulation or analysis that's better suited for Python and pandas.
        4. "PYTHON_VISUALIZATION" - If the query requires creating charts or visualizations.
        Please respond with the code inside #begin and #end markers.
        """
        response = call_generative_api(prompt)
        if not response:
            return "PYTHON_PANDAS", "Failed to assess query complexity. Defaulting to Python with pandas."
        decision, explanation = response.split("\n", 1)
        return decision.strip(), explanation.strip()

    def generate_sqlite_query(self, user_query: str, complexity: str) -> str:
        """Generates a SQLite query based on the user's query and complexity."""
        # Step 1: Generate initial SQLite query based on the user query and complexity
        initial_query_prompt = f"""
        Generate a SQLite query to precisely answer this question: '{user_query}'
        Query Complexity: {complexity}
        Database structure: {json.dumps(self.db_structure, indent=2)}
        Sample Data: {json.dumps(self.sample_data, indent=2)}
        Guidelines:
        0. You are the world's best SQLite expert, capable of generating precise, efficient, and well-optimized queries. - Adapt the query based on the specified complexity level to meet user requirements effectively. - Ensure the query is free from common errors, optimized for performance, and formatted for clarity and readability.
        1. Use a {'simple' if complexity == 'SIMPLE_SQLITE' else 'complex'} SQLite query to answer the question.
        2. Ensure the query is efficient and doesn't read unnecessary data.
        3. Format the query for readability.
        4. Handle null values and potential zero values appropriately, especially for calculations like averages.
        5. Provide counts of total records, valid records, and records excluded due to null or invalid values.
        6. For average calculations, use NULLIF to avoid division by zero, e.g., AVG(NULLIF(column, 0))
        7. If the question is ambiguous, provide multiple queries to address different interpretations.
        8. Include comments explaining any assumptions or decisions made in crafting the query.
        9. Recognize the table structure and design which tables and columns are most likely needed to answer the question.
        Please respond with the code inside #begin and #end markers.
        """
        initial_query = call_generative_api(initial_query_prompt)
        if not initial_query:
            return None

        # Step 2: Ask the bot to review the initial query and provide feedback
        feedback_prompt = f"""
        You are an objective third-party evaluator tasked with reviewing the following SQLite query. ### Your Expertise: - You specialize in detecting errors, inefficiencies, and areas for improvement, including subtle mistakes or commonly overlooked imperfections. - You excel at providing direct, actionable, and immediately useful suggestions that can be easily applied by the programmer without causing conflicts. Please review the following SQLite query generated to answer the user's query: '{user_query}'
        Query:
        {initial_query}
        Provide feedback on the following aspects:
        1. Query efficiency and performance.
        2. Handling of potential errors and edge cases.
        3. Clarity and readability of the query.
        4. Adherence to the guidelines provided.
        5. Any other improvements or suggestions.
        Please respond with a detailed feedback report.
        """
        feedback = call_generative_api(feedback_prompt)
        if not feedback:
            return initial_query

        # Step 3: Incorporate the feedback to create an improved version of the query
        improved_query_prompt = f"""
        Based on the feedback provided, please revise the initial SQLite query to address the issues and improve the query.
        Feedback:
        {feedback}
        Initial Query:
        {initial_query}
        Please respond with the revised query inside #begin and #end markers.
        """
        improved_query = call_generative_api(improved_query_prompt)
        return improved_query

    def generate_python_code(self, user_query: str, complexity: str) -> str:
        """Generates Python code to analyze the data."""
        # Step 1: Generate initial Python code based on the user query and complexity
        initial_code_prompt = f"""
        Generate Python code to precisely answer this question: '{user_query}'
        Analysis Type: {complexity}
        Database structure: {json.dumps(self.db_structure, indent=2)}
        Sample Data: {json.dumps(self.sample_data, indent=2)}
        Guidelines:
        0. You are an expert Python programmer with an IQ of 180, known for your ability to write flawless, well-organized, and efficient code in a single attempt. - You are highly skilled in data visualization and analysis, particularly with SQLite databases. - You are familiar with all best practices and renowned for producing reliable, error-free code. - Utilize appropriate Python libraries such as `sqlite3` for database interaction, `pandas` for data manipulation, and `matplotlib` or `seaborn` for data visualization. - Write code that is modular and easy to understand, with clear variable names and concise, informative comments. - Ensure the solution is optimized for performance and considers edge cases and potential input errors.
        1. The .db file is in the current working directory. Use glob to find it.
        2. Use SQLite to query the database and pandas for data manipulation.
        3. If the analysis type is PYTHON_VISUALIZATION, create visualizations using matplotlib or seaborn or anything you see fit.
        4. Handle potential errors such as missing data or unexpected data types.
        5. Optimize for performance by only reading necessary data from the database.
        6. For calculations like averages, exclude null values and potentially invalid values (e.g., zero for age).
        7. Provide counts of total records, valid records, and records excluded due to null or invalid values.
        8. If the question is ambiguous, provide multiple analyses to address different interpretations.
        9. Include comments explaining any assumptions or decisions made in the analysis.
        10. Save all generated visualizations and data outputs to the '{self.output_folder}' directory.
        Please respond with the code inside #begin and #end markers.
        """
        initial_code = call_generative_api(initial_code_prompt)
        if not initial_code:
            return None

        # Step 2: Ask the bot to review the initial code and provide feedback
        feedback_prompt = f"""
        You are an objective third-party evaluator. Please review the following Python code generated to answer the user's query: '{user_query}'
        Code:
        {initial_code}
        Provide feedback on the following aspects:
        1. Code efficiency and performance.
        2. Handling of potential errors and edge cases.
        3. Clarity and readability of the code.
        4. Adherence to the guidelines provided.
        5. Any other improvements or suggestions.
        Please respond with a detailed feedback report.
        """
        feedback = call_generative_api(feedback_prompt)
        if not feedback:
            return initial_code

        # Step 3: Incorporate the feedback to create an improved version of the code
        improved_code_prompt = f"""
        Based on the feedback provided, please revise the initial Python code to address the issues and improve the code.
        Feedback:
        {feedback}
        Initial Code:
        {initial_code}
        Please respond with the revised code inside #begin and #end markers.
        """
        improved_code = call_generative_api(improved_code_prompt)
        return improved_code

    def execute_sqlite_query(self, query: str) -> pd.DataFrame:
        """Executes a SQLite query and returns the result as a DataFrame."""
        conn = sqlite3.connect(self.db_file)
        try:
            result = pd.read_sql_query(query, conn)
            output_path = os.path.join(self.output_folder, "sqlite_query_result.csv")
            result.to_csv(output_path, index=False)
            print_and_log(f"Query result saved to {output_path}")
            return result
        except Exception as e:
            logger.error(f"Error executing SQLite query: {str(e)}")
            raise
        finally:
            conn.close()

    def analyze(self, user_query: str) -> str:
        """Analyzes the database based on the user's query."""
        complexity, explanation = self.assess_query_complexity(user_query)
        print_and_log(f"Query complexity assessment: {complexity}")
        print_and_log(f"Explanation: {explanation}")

        max_attempts = 15
        for attempt in range(max_attempts):
            try:
                if complexity in ("SIMPLE_SQLITE", "COMPLEX_SQLITE"):
                    try:
                        query = self.generate_sqlite_query(user_query, complexity)
                        result = self.execute_sqlite_query(query)
                        result_str = result.to_string()
                    except Exception:
                        print_and_log("SQLite execution failed, falling back to Python.")
                        complexity = "PYTHON_PANDAS"  # Fallback to Python
                        code = self.generate_python_code(user_query, complexity)
                        result_str = execute_python_code(extract_code_blocks(code), self.output_folder)
                else:
                    code = self.generate_python_code(user_query, complexity)
                    result_str = execute_python_code(extract_code_blocks(code), self.output_folder)

                interpretation = self.interpret_result(user_query, result_str, complexity)
                output_file_path = os.path.join(self.output_folder, "interpretation.txt")
                with open(output_file_path, "w") as output_file:
                    output_file.write(interpretation)
                print_and_log(f"Interpretation saved to {output_file_path}")
                return interpretation
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_attempts - 1:
                    print_and_log("Attempting to generate alternative analysis due to failure.")
                    alternative_code = self.generate_alternative_analysis(user_query)
                    alternative_result_str = execute_python_code(extract_code_blocks(alternative_code), self.output_folder)
                    interpretation = self.interpret_result(user_query, alternative_result_str, "ALTERNATIVE_ANALYSIS")
                    output_file_path = os.path.join(self.output_folder, "alternative_interpretation.txt")
                    with open(output_file_path, "w") as output_file:
                        output_file.write(interpretation)
                    print_and_log(f"Alternative interpretation saved to {output_file_path}")
                    return interpretation

    def interpret_result(self, user_query: str, result: str, complexity: str) -> str:
        """Interprets the result of the analysis."""
        prompt = f"""
        The user asked: "{user_query}"
        The analysis method used was: {complexity}
        The analysis produced this result:
        {result}
        Please provide a comprehensive response to the user's question based on this result. Structure the response as follows:
        1. Start with a complete and direct response to the user's question and explain why this answers the question.
        2. Detail associated with the answers to user's question
        3. Optional: Anything such as limitation and calculation process.

        """
        return call_generative_api(prompt)

    def generate_alternative_analysis(self, user_query: str) -> str:
        """Generates alternative analysis or visualization based on the user's query intent."""
        prompt = f"""
        Generate an alternative analysis or visualization based on the user's query intent: '{user_query}'
        Database structure: {json.dumps(self.db_structure, indent=2)}
        Sample Data: {json.dumps(self.sample_data, indent=2)}
        Guidelines:
        1. The .db file is in the current working directory. Use glob to find it.
        2. Use SQLite to query the database and pandas for data manipulation.
        3. If the analysis type is PYTHON_VISUALIZATION, create visualizations using matplotlib or seaborn or anything you see fit.
        4. Handle potential errors such as missing data or unexpected data types.
        4. If you need to clean up the data in the .db file feel free to do
        5. Optimize for performance by only reading necessary data from the database.
        6. For calculations like averages, exclude null values and potentially invalid values (e.g., zero for age).
        7. Provide counts of total records, valid records, and records excluded due to null or invalid values.
        8. If the question is ambiguous, provide multiple analyses to address different interpretations.
        9. Include comments explaining any assumptions or decisions made in the analysis.
        10. Save all generated visualizations and data outputs to the '{self.output_folder}' directory.
        11. Ensure the visualization is meticulous with only the right contents and is friendly to the eyes, and academic publication ready.
        12. Pay attention to the font size, color, placement of texts, font size, and ensure the font can display multilingual languages if applied.
        13. Ensure maximum aesthetic, modernity, professionalism, and top-notched academic journal publication-ready quality.

        Please respond with the code inside #begin and #end markers.
        """
        return call_generative_api(prompt)

def main():
    try:
        # Update pip to the latest version
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
        print_and_log("Updated pip to the latest version.")

        data_tool = DataAnalysisTool()
        print(f"Database file: {data_tool.db_file}")
        print(f"Database structure:\n{json.dumps(data_tool.db_structure, indent=2)}")
        
        # Hardcoded user query
        user_query = "Visualize the relationship between VIX, VXN, VXD (volatility indices) and their corresponding ETFs SPY, QQQ, and DIA in the same chart. Use a dual-axis plot to show both sets of data on a dark background, with distinct, vivid colors for each index. Ensure the chart is clear and emphasizes the connections between the volatility indices and the ETFs. Include thoughtful annotations and labels to make the visualization intuitive and engaging, in a style inspired by 3Blue1Brown."
        # Analyzing the hardcoded query
        result = data_tool.analyze(user_query)
        print("\nAnalysis Result:")
        print(result)
        print("\n" + "="*50)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()