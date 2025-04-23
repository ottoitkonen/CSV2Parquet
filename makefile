build-app:
	@echo "Building the project..."
	@rm -rf build dist
	@pyinstaller app/main.py --name CSV2Parquet --onefile --clean --optimize 2
	@echo "Build complete. The executable is located in the dist directory."
