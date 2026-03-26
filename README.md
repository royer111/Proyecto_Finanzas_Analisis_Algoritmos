Proyecto: Análisis Algorítmico de Series Financieras

📌 Descripción General

Este proyecto tiene como objetivo aplicar conceptos de Análisis de Algoritmos al procesamiento y estudio de series temporales financieras, enfocándose en el análisis de precios y volúmenes de activos bursátiles.

El sistema implementa un pipeline completo de datos que permite:
	•	Limpieza y normalización de datasets financieros
	•	Alineación temporal de activos
	•	Construcción de matrices comparables
	•	Análisis de similitud entre activos
	•	Evaluación del comportamiento del mercado mediante algoritmos eficientes

El proyecto fue desarrollado como parte de la asignatura Análisis de Algoritmos, aplicando estructuras de datos, complejidad computacional y procesamiento eficiente de información.

⸻

🎯 Objetivos

Objetivo General

Analizar el comportamiento y similitud entre activos financieros utilizando técnicas algorítmicas eficientes sobre series temporales.

Objetivos Específicos
	•	Construir un pipeline de procesamiento de datos financieros.
	•	Aplicar algoritmos de ordenamiento y comparación.
	•	Analizar complejidad temporal en operaciones sobre grandes datasets.
	•	Implementar métricas de similitud entre series temporales.
	•	Generar estructuras optimizadas para análisis cuantitativo.
  Datos Crudos
      ↓
DataCleaner
      ↓
Datos Limpios
      ↓
DataMerger
      ↓
Matrices Consolidadas
      ↓
SimilarityService
      ↓
Análisis de Similitud
      ↓
VolumeAnalysisService
      ↓
Resultados Analíticos

▶️ Ejecución del Proyecto

1️⃣ Clonar repositorio

git clone https://github.com/usuario/proyecto-analisis-algoritmos.git
cd proyecto-analisis-algoritmos

2️⃣ Crear entorno virtual
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows

3️⃣ Ejecutar análisis

Ejemplo:
python volume_analysis_service.py

📊 Resultados Esperados

El sistema genera:
	•	Matrices financieras alineadas
	•	Rankings de similitud entre activos
	•	Análisis de volumen del mercado
	•	Resultados comparativos entre algoritmos

⸻

📚 Aplicaciones

Este proyecto puede extenderse hacia:
	•	Trading cuantitativo
	•	Machine Learning financiero
	•	Detección de activos correlacionados
	•	Sistemas de recomendación financiera
	•	Análisis predictivo bursátil

⸻

👨‍💻 Autor

Royer García Palacio
Ingeniería de Sistemas y Computación — Colombia

⸻

📄 Licencia

Proyecto académico desarrollado con fines educativos.

⸻

⭐ Posibles Mejoras Futuras
	•	Visualización gráfica de resultados
	•	Integración con APIs financieras en tiempo real
	•	Implementación de DTW (Dynamic Time Warping)
	•	Paralelización de cálculos
	•	Dashboard interactivo

⸻

🚀 Estado del Proyecto

🟢 En desarrollo activo — expansión de análisis algorítmico y métricas de similitud.
