mk:
	pip install .. --force-reinstall && quartodoc build --verbose && quarto render