#!/usr/bin/env python3
"""
Flask API + UI for Product Relevancy Search
"""

import os
import sys
import traceback
from flask import Flask, request, jsonify
from flask_cors import CORS

# --------------------------------------------------
# PATH SETUP
# --------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, SCRIPT_DIR)

# --------------------------------------------------
# IMPORT ENGINE
# --------------------------------------------------
from global_relevancy import predict

# --------------------------------------------------
# FLASK APP
# --------------------------------------------------
app = Flask(__name__)
CORS(app)


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# --------------------------------------------------
# API ENDPOINT
# --------------------------------------------------
@app.route("/predict", methods=["POST"])
def predict_products():
    try:
        data = request.get_json(force=True)
        query = data.get("query")
        top_k = int(data.get("top_k", 5))

        if not query:
            return jsonify({"status": False, "message": "query required"}), 400

        result = predict(query, top_k=top_k)

        return jsonify({"status": True, "data": result})

    except Exception as e:
        return jsonify({
            "status": False,
            "error": str(e),
            "trace": traceback.format_exc()
        }), 500


# --------------------------------------------------
# UI PAGE
# --------------------------------------------------
@app.route("/ui", methods=["GET"])
def ui():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Product Suggestion Engine</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: linear-gradient(135deg, #3b82f6 0%, #1e40af 100%);
            min-height: 100vh;
            padding: 20px;
            color: #1f2937;
        }

        .container {
            max-width: 1100px;
            margin: 0 auto;
        }

        .header {
            text-align: center;
            color: white;
            margin-bottom: 40px;
            animation: fadeInDown 0.6s ease-out;
        }

        .header h1 {
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 10px;
            text-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }

        .header p {
            font-size: 1.1rem;
            opacity: 0.95;
            font-weight: 400;
        }

        .search-card {
            background: white;
            border-radius: 20px;
            padding: 40px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            margin-bottom: 30px;
            animation: fadeInUp 0.6s ease-out;
        }

        .input-group {
            position: relative;
        }

        .input-label {
            display: block;
            font-weight: 600;
            font-size: 0.95rem;
            color: #374151;
            margin-bottom: 12px;
        }

        textarea {
            width: 100%;
            min-height: 140px;
            padding: 18px;
            font-size: 15px;
            border: 2px solid #e5e7eb;
            border-radius: 12px;
            font-family: inherit;
            resize: vertical;
            transition: all 0.3s ease;
            background: #f9fafb;
        }

        textarea:focus {
            outline: none;
            border-color: #3b82f6;
            background: white;
            box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
        }

        .search-btn {
            width: 100%;
            margin-top: 20px;
            padding: 16px 32px;
            background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
            color: white;
            border: none;
            border-radius: 12px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(59, 130, 246, 0.4);
        }

        .search-btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
        }

        .search-btn:active {
            transform: translateY(0);
        }

        .search-btn:disabled {
            opacity: 0.6;
            cursor: not-allowed;
            transform: none;
        }

        .results-container {
            animation: fadeInUp 0.6s ease-out;
        }

        .result-card {
            background: white;
            border-radius: 16px;
            padding: 28px;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
            transition: all 0.3s ease;
            border-left: 5px solid #3b82f6;
            animation: slideInRight 0.4s ease-out;
        }

        .result-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 8px 30px rgba(0,0,0,0.12);
        }

        .result-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 16px;
            flex-wrap: wrap;
            gap: 12px;
        }

        .query-text {
            font-size: 0.9rem;
            color: #6b7280;
            font-weight: 500;
            flex: 1;
            min-width: 200px;
        }

        .badge {
            display: inline-flex;
            align-items: center;
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 0.85rem;
            font-weight: 600;
            white-space: nowrap;
        }

        .badge.relevant {
            background: linear-gradient(135deg, #10b981 0%, #059669 100%);
            color: white;
            box-shadow: 0 2px 8px rgba(16, 185, 129, 0.3);
        }

        .badge.not-relevant {
            background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
            color: white;
            box-shadow: 0 2px 8px rgba(239, 68, 68, 0.3);
        }

        .category-tag {
            display: inline-block;
            padding: 4px 12px;
            background: #f3f4f6;
            color: #6b7280;
            border-radius: 8px;
            font-size: 0.8rem;
            font-weight: 500;
            margin-bottom: 12px;
        }

        .match-info {
            background: #f9fafb;
            padding: 16px;
            border-radius: 10px;
            margin-top: 16px;
        }

        .match-title {
            font-size: 0.85rem;
            color: #6b7280;
            margin-bottom: 6px;
            font-weight: 500;
        }

        .match-value {
            font-size: 1.1rem;
            color: #111827;
            font-weight: 600;
            margin-bottom: 12px;
        }

        .score-bar-container {
            display: flex;
            align-items: center;
            gap: 12px;
            margin-top: 12px;
        }

        .score-label {
            font-size: 0.85rem;
            color: #6b7280;
            font-weight: 500;
            min-width: 120px;
        }

        .score-bar {
            flex: 1;
            height: 8px;
            background: #e5e7eb;
            border-radius: 10px;
            overflow: hidden;
            position: relative;
        }

        .score-fill {
            height: 100%;
            background: linear-gradient(90deg, #3b82f6 0%, #2563eb 100%);
            border-radius: 10px;
            transition: width 0.6s ease;
        }

        .score-value {
            font-weight: 600;
            color: #374151;
            min-width: 60px;
            text-align: right;
        }

        .loading {
            text-align: center;
            padding: 60px 20px;
            color: white;
            font-size: 1.1rem;
        }

        .spinner {
            display: inline-block;
            width: 50px;
            height: 50px;
            border: 4px solid rgba(255,255,255,0.3);
            border-top-color: white;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            margin-bottom: 16px;
        }

        .error-message {
            background: white;
            padding: 24px;
            border-radius: 16px;
            color: #dc2626;
            text-align: center;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        }

        .empty-state {
            text-align: center;
            color: white;
            padding: 60px 20px;
            font-size: 1.1rem;
            opacity: 0.9;
        }

        @keyframes fadeInDown {
            from {
                opacity: 0;
                transform: translateY(-20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        @keyframes fadeInUp {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        @keyframes slideInRight {
            from {
                opacity: 0;
                transform: translateX(20px);
            }
            to {
                opacity: 1;
                transform: translateX(0);
            }
        }

        @keyframes spin {
            to { transform: rotate(360deg); }
        }

        @media (max-width: 768px) {
            .header h1 {
                font-size: 1.8rem;
            }

            .header p {
                font-size: 1rem;
            }

            .search-card {
                padding: 24px;
            }

            .result-card {
                padding: 20px;
            }

            .result-header {
                flex-direction: column;
            }
        }
    </style>
</head>
<body>

<div class="container">
    <div class="header">
        <h1>🎯 Product Suggestion Engine</h1>
        <p>AI-powered product recommendations based on your problem statement</p>
    </div>

    <div class="search-card">
        <div class="input-group">
            <label class="input-label">Problem Statement / Requirements</label>
            <textarea 
                id="query" 
                placeholder="Describe your requirements, tender items, or product needs in detail...
Example: 'Need industrial safety equipment for construction site including helmets and protective gear'"
            ></textarea>
        </div>
        <button class="search-btn" onclick="search()" id="searchBtn">
            🔍 Find Matching Products
        </button>
    </div>

    <div id="result"></div>
</div>

<script>
async function search() {
    const query = document.getElementById("query").value.trim();
    const resultDiv = document.getElementById("result");
    const searchBtn = document.getElementById("searchBtn");
    
    if (!query) {
        resultDiv.innerHTML = '<div class="error-message">⚠️ Please enter a problem statement or requirement</div>';
        return;
    }

    searchBtn.disabled = true;
    searchBtn.textContent = "⏳ Analyzing...";
    
    resultDiv.innerHTML = `
        <div class="loading">
            <div class="spinner"></div>
            <div>Analyzing your requirements and finding the best matches...</div>
        </div>
    `;

    try {
        const res = await fetch("/predict", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({ query: query, top_k: 5 })
        });

        const data = await res.json();

        if (!data.status) {
            resultDiv.innerHTML = '<div class="error-message">❌ An error occurred while processing your request. Please try again.</div>';
            return;
        }

        let html = '<div class="results-container">';

        if (!data.data.results || data.data.results.length === 0) {
            html = '<div class="empty-state">No matching products found. Try refining your query.</div>';
        } else {
            data.data.results.forEach((r, index) => {
                const score = r.relevancy_score || 0;
                const isRelevant = score >= 0.65;
                const scorePercent = (score * 100).toFixed(1);

                html += `
                <div class="result-card" style="animation-delay: ${index * 0.1}s">
                    <div class="result-header">
                        <div class="query-text">
                            <strong>Query ${index + 1}:</strong> ${escapeHtml(r.query)}
                        </div>
                        <div class="badge ${isRelevant ? "relevant" : "not-relevant"}">
                            ${isRelevant ? "✓ Relevant Match" : "✗ Not Relevant"}
                        </div>
                    </div>
                    
                    ${r.detected_category ? `<div class="category-tag">📦 ${escapeHtml(r.detected_category)}</div>` : ''}
                    
                    <div class="match-info">
                        <div class="match-title">Recommended Product</div>
                        <div class="match-value">${escapeHtml(r.best_match?.title || "No matching product found")}</div>
                        
                        <div class="score-bar-container">
                            <span class="score-label">Relevancy Score</span>
                            <div class="score-bar">
                                <div class="score-fill" style="width: ${scorePercent}%"></div>
                            </div>
                            <span class="score-value">${scorePercent}%</span>
                        </div>
                    </div>
                </div>
                `;
            });
        }

        html += '</div>';
        resultDiv.innerHTML = html;

    } catch (error) {
        resultDiv.innerHTML = '<div class="error-message">❌ Network error. Please check your connection and try again.</div>';
    } finally {
        searchBtn.disabled = false;
        searchBtn.textContent = "🔍 Find Matching Products";
    }
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

document.getElementById("query").addEventListener("keydown", function(e) {
    if (e.ctrlKey && e.key === "Enter") {
        search();
    }
});
</script>

</body>
</html>
"""


# --------------------------------------------------
# RUN SERVER
# --------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)