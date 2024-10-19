import os
import json
import logging
import time
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig
from prompts import text1, text2, text3, text4, text5, text6, text7
from tqdm import tqdm
from google.api_core import retry, exceptions

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

response_schema = {
    "type": "object",
    "properties": {
        "unexpectedness_rating": {"type": "string"},
        "emotional_intensity": {"type": "string"},
        "timecode": {"type": "string"},
        "expectation_description": {"type": "string"},
        "violation_description": {"type": "string"},
        "expectation_probability": {"type": "string"},
        "sexual_content_rating": {"type": "string"}
    },
    "required": [
        "unexpectedness_rating",
        "emotional_intensity",
        "timecode",
        "expectation_description",
        "violation_description",
        "expectation_probability",
        "sexual_content_rating"
    ]
}

def safe_convert(value, convert_func):
    if value is None or value == "N/A":
        return "N/A"
    if isinstance(value, list):
        logging.warning(f"Unexpected list value: {value}")
        return "N/A"
    if isinstance(value, str) and value.lower() in ['true', 'false']:
        value = '1' if value.lower() == 'true' else '0'
    try:
        return convert_func(value)
    except (ValueError, TypeError) as e:
        logging.warning(f"Conversion error: {e} for value: {value}")
        return "N/A"

def process_analysis(analysis):
    logging.debug(f"Original analysis: {analysis}")

    processed = {}
    for field in response_schema['required']:
        value = analysis.get(field, 'N/A')
        if field in ['unexpectedness_rating', 'emotional_intensity', 'sexual_content_rating']:
            processed[field] = safe_convert(value, int)
        elif field in ['expectation_probability']:
            processed[field] = safe_convert(value, float)
        else:
            processed[field] = str(value) if value is not None else 'N/A'

    logging.debug(f"Processed analysis: {processed}")
    return processed

@retry.Retry(predicate=retry.if_exception_type(exceptions.ResourceExhausted))
def generate_content_with_retry(model, instructions, generation_config):
    return model.generate_content(
        instructions,
        generation_config=generation_config,
        stream=False,
    )

def generate(video_files, bucket_name, temperature=0.01, top_p=0.99):
    try:
        model = GenerativeModel("gemini-1.5-pro-002")
        all_results = []

        with tqdm(total=len(video_files), desc=f"Processing videos (temp={temperature}, top_p={top_p})") as pbar:
            for video_file in video_files:
                video_uri = f"gs://{bucket_name}/{video_file}"
                video_part = Part.from_uri(mime_type="video/mp4", uri=video_uri)

                try:
                    logging.info(f"Processing video: {video_file}")

                    instructions = [
                        video_part,
                        """
                        Analyze the video and provide your response strictly in valid JSON format as per the provided schema, without any additional text, explanations, or formatting symbols like asterisks. Do not include markdown or any other markup language in your response. If a value is not applicable or cannot be determined, use 'N/A'.
                        """,
                        text1, text2, text3, text4, text5, text6, text7
                    ]

                    generation_config = GenerationConfig(
                        max_output_tokens=8000,
                        temperature=temperature,
                        top_p=top_p,
                        seed=1,
                        response_mime_type="application/json",
                        response_schema=response_schema
                    )

                    response = generate_content_with_retry(model, instructions, generation_config)

                    logging.debug(f"Raw response: {response.text}")

                    try:
                        analysis = json.loads(response.text)
                    except json.JSONDecodeError as e:
                        logging.error(f"JSON decode error: {e}")
                        logging.error(f"Raw response causing error: {response.text}")
                        continue

                    analysis = process_analysis(analysis)
                    video_filename = os.path.basename(video_file)
                    video_id_str = os.path.splitext(video_filename)[0]

                    try:
                        analysis['id'] = int(video_id_str)
                    except ValueError as e:
                        logging.error(f"Invalid video ID {video_id_str}: {e}")
                        continue  # Skip this video if the ID is not a valid integer

                    logging.info(f"Final analysis for {video_file}:\n{json.dumps(analysis, indent=2)}")
                    print("-" * 50)

                    all_results.append(analysis)

                except exceptions.ResourceExhausted as e:
                    logging.warning(f"Resource exhausted for video {video_file}. Retrying after delay: {e}")
                    time.sleep(15)  # Wait for 15 seconds before retrying
                    pbar.update(0)  # Update progress bar without incrementing
                    continue

                except Exception as e:
                    logging.exception(f"Error processing video {video_file}: {e}")

                finally:
                    pbar.update(1)
                    time.sleep(1)  # Add a small delay between each video processing

        return all_results

    except Exception as e:
        logging.exception(f"Error in generate function: {e}")
        return []