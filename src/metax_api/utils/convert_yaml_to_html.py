import logging
import json
import yaml
from os import path, makedirs
from django.conf import settings

_logger = logging.getLogger(__name__)

TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Swagger UI</title>
  <link href="https://fonts.googleapis.com/css?family=Open+Sans:400,700|Source+Code+Pro:300,600|Titillium+Web:400,600,700" rel="stylesheet">
  <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.24.2/swagger-ui.css" >
  <style>
    html
    {
      box-sizing: border-box;
      overflow: -moz-scrollbars-vertical;
      overflow-y: scroll;
    }
    *,
    *:before,
    *:after
    {
      box-sizing: inherit;
    }

    body {
      margin:0;
      background: #fafafa;
    }
  </style>
</head>
<body>

<div id="swagger-ui"></div>

<script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.24.2/swagger-ui-bundle.js"> </script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.24.2/swagger-ui-standalone-preset.js"> </script>
<script>
window.onload = function() {

  var spec = %s;

  // Build a system
  const ui = SwaggerUIBundle({
    spec: spec,
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset
    ],
    plugins: [
      SwaggerUIBundle.plugins.DownloadUrl
    ],
    layout: "StandaloneLayout"
  })

  window.ui = ui
}
</script>
</body>

</html>
"""

FAIL_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Swagger file creation failed</title>
</head>
<body>
    <h2>Swagger file creation failed!</h2>
</body>
</html>
"""


def yaml_to_html_convert():

    try:
        for api_version in ["v1", "v2"]:
            inpath = path.join(settings.SWAGGER_YAML_PATH, api_version, 'swagger.yaml')
            outpath = path.join(settings.SWAGGER_HTML_PATH, api_version, 'swagger.html')

            if not path.exists(path.dirname(outpath)):
                try:
                    makedirs(path.dirname(outpath))
                except OSError as exc:
                    _logger.error(exc)
                    raise

            with open(outpath, 'w') as outfile:
                try:
                    with open(inpath, 'r') as infile:
                        readdata = infile.read()
                        indata = readdata.replace("__METAX_ENV_DOMAIN__", settings.SERVER_DOMAIN_NAME)
                        spec = yaml.load(indata, Loader=yaml.FullLoader)
                        outfile.write(TEMPLATE % json.dumps(spec))

                except FileNotFoundError:
                    outfile.write(FAIL_TEMPLATE)
                except yaml.YAMLError as exc:
                    _logger.error("YAML loading failed")
                    _logger.error(exc)
                    outfile.write(FAIL_TEMPLATE)
                except json.decoder.JSONDecodeError as exc:
                    _logger.error("JSON loading failed")
                    _logger.error(exc)

    except PermissionError:
        _logger.error("Permission error")
