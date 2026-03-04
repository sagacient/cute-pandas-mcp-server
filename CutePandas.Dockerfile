# CutePandas - Pre-built pandas execution environment for MCP servers
# Local build fallback for cute-pandas-mcp-server (BUILD_LOCAL=true)
# Synced with cutepandas/Dockerfile

FROM python:3.12-slim

LABEL org.opencontainers.image.title="CutePandas"
LABEL org.opencontainers.image.description="Pre-built pandas execution environment for MCP servers"
LABEL org.opencontainers.image.licenses="MPL-2.0"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies for some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    libgl1 \
    libglib2.0-0 \
    libicu-dev \
    libgomp1 \
    gdal-bin \
    libgdal-dev \
    libproj-dev \
    libgeos-dev \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages - let pip resolve compatible versions
RUN pip install --no-cache-dir \
    'pandas>=2.2.0,<3.0' \
    'numpy>=1.26.0,<2.0' \
    'openpyxl>=3.1.0' \
    'xlrd>=2.0.0' \
    'pyarrow>=19.0.0' \
    'fastparquet>=2024.11.0' \
    'scipy>=1.15.0' \
    'scikit-learn>=1.6.0' \
    'matplotlib>=3.10.0' \
    'seaborn>=0.13.0' \
    'tabulate>=0.9.0' \
    'python-docx>=1.1.0' \
    'pypdf>=5.0.0' \
    'reportlab>=4.2.0' \
    'python-pptx>=1.0.0' \
    'beautifulsoup4>=4.12.0' \
    'lxml>=5.3.0' \
    'Pillow>=11.0.0' \
    'chardet>=5.2.0' \
    'odfpy>=1.4.0' \
    'pyyaml>=6.0' \
    'toml>=0.10.0' \
    'markdown>=3.7' \
    'opencv-python-headless>=4.10.0' \
    'pytesseract>=0.3.0' \
    'pdf2image>=1.17.0' \
    'scikit-image>=0.24.0' \
    'imageio>=2.36.0' \
    'sympy>=1.13.0' \
    'mpmath>=1.3.0' \
    'pylatexenc>=2.10' \
    'spacy>=3.8.0,<4.0' \
    'nltk>=3.9.0' \
    'textblob>=0.18.0' \
    'langdetect>=1.0.9' \
    'gensim>=4.3.0' \
    'rapidfuzz>=3.10.0' \
    'ftfy>=6.3.0' \
    'unidecode>=1.3.0' \
    'regex>=2024.11.0' \
    'textstat>=0.7.0' \
    'wordcloud>=1.9.0' \
    'emoji>=2.14.0' \
    'polyglot>=16.7.0' \
    'geopandas>=1.0.0' \
    'shapely>=2.0.0' \
    'pyproj>=3.7.0' \
    'geodatasets>=2023.12.0' \
    'geopy>=2.4.0' \
    'folium>=0.18.0' \
    'contextily>=1.6.0' \
    'rtree>=1.3.0' \
    'h3>=3.7.0' \
    'geojson>=3.1.0' \
    'pygeohash>=1.2.0' \
    'haversine>=2.8.0' \
    'duckdb>=1.2.0' \
    'polars>=1.24.0' \
    'statsmodels>=0.14.0' \
    'xgboost>=2.1.0'

# Create non-root user for security
RUN useradd -m -s /bin/bash -u 1000 pandas

# Create directories
RUN mkdir -p /data /output && \
    chown -R pandas:pandas /data /output

# Switch to non-root user
USER pandas

# Set working directory
WORKDIR /home/pandas

# Default command
ENTRYPOINT ["python"]
