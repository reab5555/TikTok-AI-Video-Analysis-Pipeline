# TikTok Analytics Pipeline

## Introduction

This project is designed to build a comprehensive analytics pipeline using Google Cloud for evaluating TikTok videos based on various engagement KPIs and AI-derived analyzed video content metrics such as unexpected or surprising elements. This pipeline effectively integrates video data extraction, storage, transformation, AI processing, and data warehousing.

## Overview

The primary goal of this project is to construct a robust data pipeline capable of providing insightful analytics on TikTok videos by examining both engagement metrics and AI-generated interpretative insights. Specifically, the project seeks to understand whether the conceptual elements of surprise and emotional intensity in video content correlate with traditional engagement KPIs such as views, likes, shares, and comments. To achieve this, the project integrates several cutting-edge technologies and methodologies:

    Data Consolidation and Storage: Collect raw video data and metadata from the TikTok API, storing them efficiently in the highly scalable Google Cloud Storage. This forms the basis for subsequent data processing and analytical ventures.

    AI-Driven Video Analysis: Leverage the Gemini LLM via Vertex AI to extract nuanced interpretations of videos, focusing on ratings of unexpectedness and emotional intensity. By capturing these subtle metrics, the aim is to enrich the analytical narrative beyond standard engagement figures.

    Data Transformation and Warehousing: Transform the raw and AI-processed data into structured formats suitable for comprehensive analysis. This involves merging AI outputs with video metadata into a star schema within BigQuery, where data is organized to optimize efficiency and accessibility for analytical tasks.

    Insight Derivation and Validation: Use advanced data analysis to explore and quantify the relationship between a video's surprising content and its engagement KPIs. This aspect of the project includes validating AI model outputs against predetermined benchmarks and psychological insights on surprise, achieved through RAG techniques.

Ultimately, the project endeavors to provide a unique insight into the impact of content creativity and emotional resonance on social media engagement. This approach positions the project at the intersection of psychology, data science, and digital marketing, aiming to enhance understanding of audience interactions and content efficacy on platforms like TikTok.


### Google Cloud Services   
Storage: Google Cloud Storage is used to house raw video files and their metadata, facilitating subsequent processing phases.   
Vertex AI: Utilized for running the Gemini LLM model, performing video analysis by generating synthetic insights on emotional intensity and unexpectedness ratings.   
BigQuery: Acts as the data warehouse where processed data is loaded. It supports the creation of a star schema to store transformed data combining both TikTok metadata and AI-generated insights.   

Google Cloud Client Libraries: Specifically, storage and bigquery packages for interacting with Google Cloud services.

## Detailed Pipeline Flow

### Data Extraction and Storage 
The TikTok API is used to fetch videos and metadata which are stored in a Google Cloud Storage bucket. Video files are downloaded and directly uploaded into GCS.

### AI Processing 
The Vertex AI platform is set up with the Gemini LLM, analyzing individual video files. The AI model is tasked with determining metrics such as unexpectedness, emotional intensity, and expectation gaps using structured prompts. These insights are produced in a JSON schema format, ensuring interoperability with subsequent data processing tools.

### Data Transformation and Loading:   
Processed insights from the Gemini LLM, along with TikTok metadata, are transferred to a BigQuery data warehouse where data is organized into a star schema. The schema includes dimension tables (dim_user, dim_video, dim_lang) and a fact table (fact_video_analytics), capturing both engagement KPIs and AI-derived metrics.

### Analysis and Insights   
The resulting structured tables allow for in-depth analysis of how unexpectedness ratings and emotional intensity may correlate with traditional engagement metrics such as video views, likes, comments, and shares. This integration facilitates insights on the content aspects that drive viewer interaction.

## Conclusion
This project showcases an integration of cloud services to transform TikTok video data into a structured format that blends both hard engagement metrics and soft AI-derived insights. The pipeline not only aids in assessing video performance but also offers a novel way of interpreting video content through the lens of audience surprise and emotional engagement.
