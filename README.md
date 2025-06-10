# AWS Glue Social Media Sentiment Analysis

This project performs sentiment analysis on raw social media JSON data using AWS Glue and PySpark.

## 🧠 Workflow
1. **Raw Input**: Social media posts (JSON with emojis, hashtags, and noise) in S3.
2. **Glue Crawler**: Catalogs the raw data.
3. **Glue ETL Job**: 
   - Cleans and normalizes text.
   - Applies sentiment scoring using a basic dictionary.
   - Outputs cleaned, scored JSON to another S3 bucket.

## 📁 Directory Structure

