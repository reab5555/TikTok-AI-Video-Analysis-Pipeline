import os
import json
import logging
import time
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig
from prompts import text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12
from tqdm import tqdm
from google.api_core import retry, exceptions

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

response_schema = {
    "type": "object",
    "properties": {
        "ai_unexpectedness_rating": {"type": "string"},  # text1
        "ai_unexpectedness_duration": {"type": "string"},  # text2
        "ai_expectation_violation_description": {"type": "string"},  # text3
        "ai_emotional_intensity": {"type": "string"},  # text4
        "ai_positivity": {"type": "string"},  # text5
        "ai_negativity": {"type": "string"},  # text6
        "ai_expected_desirability": {"type": "string"},  # text7
        "ai_unexpected_desirability": {"type": "string"},  # text8
        "ai_emotional_spatial_closeness": {"type": "string"},  # text9
        "ai_cognitive_interruption": {"type": "string"},  # text10
        "ai_perceived_realism": {"type": "string"},  # text11
        "ai_sexual_content_rating": {"type": "string"}  # text12
    },
    "required": [
        "ai_unexpectedness_rating",
        "ai_unexpectedness_duration",
        "ai_expectation_violation_description",
        "ai_emotional_intensity",
        "ai_positivity",
        "ai_negativity",
        "ai_expected_desirability",
        "ai_unexpected_desirability",
        "ai_emotional_spatial_closeness",
        "ai_cognitive_interruption",
        "ai_perceived_realism",
        "ai_sexual_content_rating"
    ]
}


def extract_video_id(filename):
    """Extract the numeric video ID from the filename, handling 'Copy of' prefix."""
    # Remove file extension
    base_name = os.path.splitext(filename)[0]

    # Remove 'Copy of ' prefix if present
    if base_name.startswith('Copy of '):
        base_name = base_name[8:]

    try:
        # Extract numeric ID
        return int(base_name)
    except ValueError as e:
        logging.error(f"Could not extract valid numeric ID from {filename}: {e}")
        return None


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
        if field in [
            'ai_unexpectedness_rating',  # text1
            'ai_unexpectedness_duration',  # text2
            'ai_emotional_intensity',  # text4
            'ai_positivity',  # text5
            'ai_negativity',  # text6
            'ai_expected_desirability',  # text7
            'ai_unexpected_desirability',  # text8
            'ai_emotional_spatial_closeness',  # text9
            'ai_cognitive_interruption',  # text10
            'ai_perceived_realism',  # text11
            'ai_sexual_content_rating'  # text12
        ]:
            processed[field] = safe_convert(value, int)
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
                        text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12
                    ]

                    generation_config = GenerationConfig(
                        max_output_tokens=8000,
                        temperature=temperature,
                        top_p=top_p,
                        seed=42,
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
                    video_id = extract_video_id(os.path.basename(video_file))

                    if video_id is not None:
                        analysis['video_id'] = video_id
                        logging.info(f"Final analysis for {video_file}:\n{json.dumps(analysis, indent=2)}")
                        print("-" * 50)
                        all_results.append(analysis)
                    else:
                        logging.error(f"Skipping video {video_file} due to invalid ID format")

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