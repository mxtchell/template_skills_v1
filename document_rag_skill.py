from __future__ import annotations
from types import SimpleNamespace
from typing import List, Optional, Dict, Any

import pandas as pd
from skill_framework import SkillInput, SkillVisualization, skill, SkillParameter, SkillOutput, ParameterDisplayDescription
from skill_framework.skills import ExportData

import requests
import json
import os
from jinja2 import Template
import base64
import io
from PIL import Image
import logging
import re
import html

logger = logging.getLogger(__name__)

@skill(
    name="Document RAG Explorer",
    llm_name="document_rag_explorer",
    description="Retrieves and analyzes relevant documents from knowledge base to answer user questions",
    capabilities="Searches through uploaded documents, finds relevant passages, generates comprehensive answers with citations, and provides source visualizations",
    limitations="Limited to documents in the knowledge base, requires pre-processed document chunks in pack.json",
    example_questions="What does the documentation say about X? Find information about Y in the documents. Explain concept Z from the knowledge base.",
    parameter_guidance="Enter your question to search the knowledge base. The system will find relevant documents and generate a comprehensive answer.",
    parameters=[
        SkillParameter(
            name="user_question",
            description="The question to answer using the knowledge base",
            required=True
        ),
        SkillParameter(
            name="base_url",
            description="Base URL for document links (e.g., https://your-domain.com/knowledge-base/)",
            required=True
        ),
        SkillParameter(
            name="max_sources",
            description="Maximum number of source documents to include",
            default_value=5
        ),
        SkillParameter(
            name="match_threshold",
            description="Minimum similarity score for document matching (0-1)",
            default_value=0.3
        ),
        SkillParameter(
            name="max_characters",
            description="Maximum characters to include from sources",
            default_value=3000
        )
    ]
)
def document_rag_explorer(parameters: SkillInput):
    """Main skill function for document RAG exploration"""
    
    # Get parameters
    user_question = parameters.arguments.user_question
    base_url = parameters.arguments.base_url
    max_sources = parameters.arguments.max_sources or 5
    match_threshold = parameters.arguments.match_threshold or 0.3
    max_characters = parameters.arguments.max_characters or 3000
    
    # Get globals if available (for topics list from previous components)
    list_of_topics = getattr(parameters.globals, 'list_of_topics', [])
    
    # Initialize results
    main_html = ""
    sources_html = ""
    title = "Document Analysis"
    
    try:
        # Load document sources from pack.json
        loaded_sources = load_document_sources()
        
        if not loaded_sources:
            return SkillOutput(
                narrative="No document sources found. Please ensure pack.json is available.",
                visualizations=[],
                export_data=[]
            )
        
        # Find matching documents
        docs = find_matching_documents(
            user_question=user_question,
            topics=list_of_topics,
            loaded_sources=loaded_sources,
            base_url=base_url,
            max_sources=max_sources,
            match_threshold=match_threshold,
            max_characters=max_characters
        )
        
        if not docs:
            # No results found
            no_results_html = """
            <div style="text-align: center; padding: 40px; color: #666;">
                <h2>No relevant documents found</h2>
                <p>No documents in the knowledge base matched your question with sufficient relevance.</p>
                <p>Try rephrasing your question or using different keywords.</p>
            </div>
            """
            main_html = no_results_html
            sources_html = "<p>No sources available</p>"
            title = "No Results Found"
        else:
            # Generate response from documents
            response_data = generate_rag_response(user_question, docs)
            
            # Create main response HTML (without sources section)
            if response_data:
                main_html = force_ascii_replace(
                    Template(main_response_template).render(
                        title=response_data['title'],
                        content=response_data['content']
                    )
                )
                
                # Create separate sources HTML
                sources_html = force_ascii_replace(
                    Template(sources_template).render(
                        references=response_data['references']
                    )
                )
                title = response_data['title']
            else:
                main_html = "<p>Error generating response from documents.</p>"
                sources_html = "<p>Error loading sources</p>"
                title = "Error"
    
    except Exception as e:
        logger.error(f"Error in document RAG: {str(e)}")
        main_html = f"<p>Error processing request: {str(e)}</p>"
        sources_html = "<p>Error loading sources</p>"
        title = "Error"
    
    # Create two visualizations - main response and sources
    visualizations = [
        SkillVisualization(
            title=title,
            layout=main_html
        ),
        SkillVisualization(
            title="Sources",
            layout=sources_html
        )
    ]
    
    # Return skill output
    return SkillOutput(
        narrative=f"Found {len(docs) if 'docs' in locals() else 0} relevant documents for your question.",
        visualizations=visualizations,
        export_data=[]
    )

# Helper Functions and Templates

def load_document_sources():
    """Load document sources from pack.json in skill resources"""
    loaded_sources = []
    
    try:
        # Look for pack.json in skill resources directory
        # This should be in the same directory as the skill or in a resources folder
        possible_paths = [
            os.path.join(os.path.dirname(__file__), "pack.json"),
            os.path.join(os.path.dirname(__file__), "resources", "pack.json"),
            os.path.join(os.path.dirname(__file__), "..", "resources", "pack.json")
        ]
        
        pack_file = None
        for path in possible_paths:
            if os.path.exists(path):
                pack_file = path
                break
        
        if pack_file:
            logger.info(f"Loading documents from: {pack_file}")
            with open(pack_file, 'r', encoding='utf-8') as f:
                resource_contents = json.load(f)
                
                # Handle different pack.json formats
                if isinstance(resource_contents, list):
                    # Format: [{"File": "doc.pdf", "Chunks": [{"Text": "...", "Page": 1}]}]
                    for processed_file in resource_contents:
                        file_name = processed_file.get("File", "unknown_file")
                        for chunk in processed_file.get("Chunks", []):
                            res = {
                                "file_name": file_name,
                                "text": chunk.get("Text", ""),
                                "description": chunk.get("Text", "")[:200] + "..." if len(chunk.get("Text", "")) > 200 else chunk.get("Text", ""),
                                "chunk_index": chunk.get("Page", 1),
                                "citation": file_name
                            }
                            loaded_sources.append(res)
                else:
                    logger.warning("Unexpected pack.json format - expected array of files")
        else:
            logger.warning("pack.json not found in expected locations")
            
    except Exception as e:
        logger.error(f"Error loading pack.json: {str(e)}")
    
    logger.info(f"Loaded {len(loaded_sources)} document chunks from pack.json")
    return loaded_sources

def find_matching_documents(user_question, topics, loaded_sources, base_url, max_sources, match_threshold, max_characters):
    """Find documents matching the user question using simple text matching"""
    # For a full implementation, you would use embedding-based matching
    # This is a simplified version using text similarity
    
    matches = []
    chars_so_far = 0
    
    # Combine question and topics for searching
    search_terms = [user_question] + topics
    
    for source in loaded_sources:
        if len(matches) >= max_sources or chars_so_far >= max_characters:
            break
            
        # Simple relevance scoring (in production, use embeddings)
        score = calculate_simple_relevance(source['text'], search_terms)
        
        if score >= match_threshold:
            source['match_score'] = score
            # Build full URL for the document page
            source['url'] = f"{base_url.rstrip('/')}/{source['file_name']}#page={source['chunk_index']}"
            matches.append(source)
            chars_so_far += len(source['text'])
    
    # Sort by score
    matches.sort(key=lambda x: x['match_score'], reverse=True)
    
    # Convert to SimpleNamespace for compatibility
    return [SimpleNamespace(**match) for match in matches[:max_sources]]

def calculate_simple_relevance(text, search_terms):
    """Calculate simple relevance score (placeholder for embedding similarity)"""
    text_lower = text.lower()
    score = 0.0
    
    for term in search_terms:
        if term and term.lower() in text_lower:
            # Count occurrences and normalize
            occurrences = text_lower.count(term.lower())
            score += min(occurrences * 0.1, 0.5)
    
    return min(score, 1.0)

def generate_rag_response(user_question, docs):
    """Generate response using LLM with document context"""
    if not docs:
        return None
    
    # Build facts from documents for LLM prompt
    facts = []
    for i, doc in enumerate(docs):
        facts.append(f"====== Source {i+1} ====")
        facts.append(f"File and page: {doc.file_name} page {doc.chunk_index}")
        facts.append(f"Description: {doc.description}")
        facts.append(f"Citation: {doc.url}")
        facts.append(f"Content: {doc.text}")
        facts.append("")
    
    # Create the prompt for the LLM
    prompt_template = Template(narrative_prompt)
    full_prompt = prompt_template.render(
        user_query=user_question,
        facts="\n".join(facts)
    )
    
    try:
        # TODO: Replace with actual LLM call
        # For now, create a structured response based on the sources
        title = f"Analysis: {user_question}"
        
        # Build content with citations
        content_parts = [
            f"<p>Based on the available documents, here's what I found regarding: <strong>{user_question}</strong></p>"
        ]
        
        for i, doc in enumerate(docs):
            content_parts.append(f"<p>{doc.text[:200]}...<sup>[{i+1}]</sup></p>")
        
        content = "\n".join(content_parts)
        
        # Build references with actual URLs and thumbnails
        references = []
        for i, doc in enumerate(docs):
            # Create preview text (first 120 characters)
            preview_text = doc.text[:120] + "..." if len(doc.text) > 120 else doc.text
            
            ref = {
                'number': i + 1,
                'url': doc.url,
                'src': doc.file_name,
                'page': doc.chunk_index,
                'text': f"Document: {doc.file_name}",
                'preview': preview_text,
                'thumbnail': ""  # Would be populated with actual thumbnail if available
            }
            references.append(ref)
        
        return {
            'title': title,
            'content': content,
            'references': references,
            'raw_prompt': full_prompt  # For debugging
        }
        
    except Exception as e:
        logger.error(f"Error generating LLM response: {str(e)}")
        return {
            'title': 'Error Processing Documents',
            'content': f'<p>Error generating response: {str(e)}</p>',
            'references': []
        }

def force_ascii_replace(html_string):
    """Clean HTML string for safe rendering"""
    # Remove null characters
    cleaned = html_string.replace('\u0000', '')
    
    # Escape special characters, but preserve existing HTML entities
    cleaned = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)', '&amp;', cleaned)
    
    # Replace problematic characters with HTML entities
    cleaned = cleaned.replace('"', '&quot;')
    cleaned = cleaned.replace("'", '&#39;')
    cleaned = cleaned.replace('–', '&ndash;')
    cleaned = cleaned.replace('—', '&mdash;')
    cleaned = cleaned.replace('…', '&hellip;')
    
    # Convert curly quotes to straight quotes
    cleaned = cleaned.replace('"', '"').replace('"', '"')
    cleaned = cleaned.replace(''', "'").replace(''', "'")
    
    # Remove any remaining control characters
    cleaned = ''.join(ch for ch in cleaned if ord(ch) >= 32 or ch in '\n\r\t')
    
    return cleaned

# HTML Templates

narrative_prompt = """
Answer the user's question based on the sources provided by writing a short headline between <title> tags then detail the supporting info for that answer in HTML between <content> tags.  The content should contain citation references like <sup>[source number]</sup> where appropriate.  Conclude with a list of the references in <reference> tags like the example.

Base your summary solely on the provided facts, avoiding assumptions.

### EXAMPLE
example_question: Why are clouds so white

====== Example Source 1 ====
File and page: cloud_info_doc.pdf page 1
Description: A document about clouds
Citation: https://superstoredev.local.answerrocket.com:8080/apps/chat/knowledge-base/5eea3d30-8e9e-4603-ba27-e12f7d51e372#page=1
Content: Clouds appear white because of how they interact with light. They consist of countless tiny water droplets or ice crystals that scatter all colors of light equally. When sunlight, which contains all colors of the visible spectrum, hits these particles, it scatters in all directions. This scattered light combines to appear white to our eyes. 
====== example Source 2 ====
File and page: cloud_info_doc.pdf page 3
Description: A document about clouds
Citation: https://superstoredev.local.answerrocket.com:8080/apps/chat/knowledge-base/5eea3d30-8e9e-4603-ba27-e12f7d51e372#page=3
Content: clouds contain millions of water droplets or ice crystals that act as tiny reflectors. the size of the water droplets or ice crystals is large enough to scatter all colors of light, unlike the sky which scatters blue light more. these particles scatter all wavelengths of visible light equally, resulting in white light. 

example_assistant: <title>The reason for white clouds</title>
<content>
    <p>Clouds appear white because of the way they interact with light. They are composed of tiny water droplets or ice crystals that scatter all colors of light equally. When sunlight, which contains all colors of the visible spectrum, hits these particles, they scatter the light in all directions. This scattered light combines to appear white to our eyes.<sup>[1]</sup></p>
    
    <ul>
        <li>Clouds contain millions of water droplets or ice crystals that act as tiny reflectors.<sup>[2]</sup></li>
        <li>These particles scatter all wavelengths of visible light equally, resulting in white light.<sup>[2]</sup></li>
        <li>The size of the water droplets or ice crystals is large enough to scatter all colors of light, unlike the sky which scatters blue light more.<sup>[2]</sup></li>
    </ul>
</content>
<reference number=1 url="https://superstoredev.local.answerrocket.com:8080/apps/chat/knowledge-base/5eea3d30-8e9e-4603-ba27-e12f7d51e372#page=1" doc="cloud_info_doc.pdf" page=1>Clouds are made of tiny droplets</reference>
<reference number=2 url="https://superstoredev.local.answerrocket.com:8080/apps/chat/knowledge-base/5eea3d30-8e9e-4603-ba27-e12f7d51e372#page=3" doc="cloud_info_doc.pdf" page=3>Ice crystals scatter all colors</reference>

### The User's Question to Answer 
Answer this question: {{user_query}}

{{facts}}"""

# Main response template (without sources)
main_response_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{{ title }}</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background-color: #fff;
        }
        .headline {
            font-size: 24px;
            font-weight: 600;
            color: #1a1a1a;
            margin-bottom: 20px;
            border-bottom: 2px solid #e1e5e9;
            padding-bottom: 10px;
        }
        .content {
            margin-bottom: 40px;
            font-size: 16px;
            line-height: 1.7;
        }
        .content p {
            margin-bottom: 16px;
        }
        sup {
            color: #0066cc;
            font-weight: 600;
        }
    </style>
</head>
<body>
    <div class="headline">{{ title }}</div>
    <div class="content">
        {{ content|safe }}
    </div>
</body>
</html>"""

# Sources template (separate tab)
sources_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sources</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 900px;
            margin: 0 auto;
            padding: 20px;
            background-color: #fff;
        }
        .sources-title {
            font-size: 18px;
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 20px;
            border-bottom: 1px solid #dee2e6;
            padding-bottom: 8px;
        }
        .source-item {
            display: flex;
            align-items: flex-start;
            margin-bottom: 20px;
            padding: 16px;
            background-color: #f8f9fa;
            border-radius: 6px;
            border: 1px solid #e9ecef;
            transition: box-shadow 0.2s ease;
        }
        .source-item:hover {
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        .source-thumbnail {
            flex-shrink: 0;
            margin-right: 16px;
        }
        .source-thumbnail img {
            width: 120px;
            height: 150px;
            object-fit: cover;
            border: 1px solid #ddd;
            border-radius: 4px;
            background-color: #f5f5f5;
        }
        .source-info {
            flex: 1;
        }
        .source-title {
            font-weight: 600;
            margin-bottom: 8px;
        }
        .source-title a {
            color: #0066cc;
            text-decoration: none;
            font-size: 16px;
        }
        .source-title a:hover {
            text-decoration: underline;
        }
        .source-meta {
            color: #666;
            font-size: 14px;
            margin-bottom: 8px;
        }
        .source-preview {
            color: #555;
            font-size: 14px;
            line-height: 1.5;
            margin-top: 8px;
        }
    </style>
</head>
<body>
    <div class="sources-title">Document Sources</div>
    {% for ref in references %}
    <div class="source-item">
        <div class="source-thumbnail">
            {% if ref.thumbnail %}
            <img src="data:image/png;base64,{{ ref.thumbnail }}" alt="Document thumbnail">
            {% else %}
            <img src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTIwIiBoZWlnaHQ9IjE1MCIgdmlld0JveD0iMCAwIDEyMCAxNTAiIGZpbGw9Im5vbmUiIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyI+CjxyZWN0IHdpZHRoPSIxMjAiIGhlaWdodD0iMTUwIiBmaWxsPSIjRjVGNUY1Ii8+CjxwYXRoIGQ9Ik00MCA2MEg4MFY2NEg0MFY2MFpNNDAgNzJIODBWNzZINDBWNzJaTTQwIDg0SDY0Vjg4SDQwVjg0WiIgZmlsbD0iI0NDQ0NDQyIvPgo8L3N2Zz4K" alt="Document placeholder">
            {% endif %}
        </div>
        <div class="source-info">
            <div class="source-title">
                <a href="{{ ref.url }}" target="_blank">[{{ ref.number }}] {{ ref.text }}</a>
            </div>
            <div class="source-meta">{{ ref.src }}, Page {{ ref.page }}</div>
            {% if ref.preview %}
            <div class="source-preview">{{ ref.preview }}</div>
            {% endif %}
        </div>
    </div>
    {% endfor %}
</body>
</html>"""

if __name__ == '__main__':
    skill_input = document_rag_explorer.create_input(
        arguments={
            "user_question": "What information is available about clouds?",
            "base_url": "https://example.com/kb/",
            "max_sources": 3,
            "match_threshold": 0.3
        }
    )
    out = document_rag_explorer(skill_input)
    print(f"Narrative: {out.narrative}")
    print(f"Visualizations: {len(out.visualizations)}")