# Continuous Intelligence

This site provides documentation for this project.
Use the navigation to explore module-specific materials.

## How-To Guide

Many instructions are common to all our projects.

See
[⭐ **Workflow: Apply Example**](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
to get these projects running on your machine.

## Project Documentation Pages (docs/)

- **Home** - this documentation landing page
- **Project Instructions** - instructions specific to this module
- **Your Files** - how to copy the example and create your version
- **Glossary** - project terms and concepts

## Additional Resources

- [Suggested Datasets](https://denisecase.github.io/pro-analytics-02/reference/datasets/cintel/)
## Custom Project

### Dataset
The dataset used in this project contains system metrics collected during two different periods. The reference dataset represents earlier system behavior, and the current dataset represents more recent behavior. Each dataset includes three main variables: requests, errors, and total latency in milliseconds.

### Signals
The main signals used are the average number of requests, average errors, and average total latency. In addition, I created new signals by calculating the percentage change for each metric. I also used drift flags for each metric and added an overall drift flag.

### Experiments
For my custom project, I created a new Python file called `drift_detector_abdellah.py`. I modified the original pipeline by adding percentage change calculations and an overall drift flag. These changes help provide more meaningful insights into how the system behavior has changed.

### Results
The results showed that the modified pipeline produces more detailed output compared to the original version. It includes not only the differences between reference and current values but also percentage changes and a global drift indicator.

### Interpretation
This project demonstrates how drift detection can be used to monitor system performance. The percentage change helps better understand the impact of changes, while the overall drift flag gives a quick summary. This can help analysts identify problems and make better decisions.
