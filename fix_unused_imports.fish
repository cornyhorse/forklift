#!/usr/bin/env fish

# Script to automatically remove unused imports (F401 errors) from Python files
# Based on flake8 output

echo "🧹 Fixing unused imports (F401 errors)..."

# src/forklift/processors/schema_validator/base_local.py:7:1: F401 'typing.Any' imported but unused
sed -i '' 's/from typing import Any, /from typing import /' src/forklift/processors/schema_validator/base_local.py
sed -i '' 's/from typing import Any$/from typing import/' src/forklift/processors/schema_validator/base_local.py
sed -i '' 's/, Any//' src/forklift/processors/schema_validator/base_local.py

# src/forklift/readers.py:9:1: F401 'typing.Any' imported but unused
# src/forklift/readers.py:9:1: F401 'typing.Dict' imported but unused
sed -i '' 's/from typing import Any, Dict, /from typing import /' src/forklift/readers.py
sed -i '' 's/from typing import Any, Dict$/from typing import/' src/forklift/readers.py
sed -i '' 's/, Any, Dict//' src/forklift/readers.py
sed -i '' 's/Any, Dict, //' src/forklift/readers.py

# src/forklift/schema/generator/core.py:3:1: F401 'json' imported but unused
sed -i '' '/^import json$/d' src/forklift/schema/generator/core.py

# src/forklift/schema/generator/core.py:5:1: F401 'datetime.datetime' imported but unused
sed -i '' 's/from datetime import datetime, /from datetime import /' src/forklift/schema/generator/core.py
sed -i '' 's/from datetime import datetime$/from datetime import/' src/forklift/schema/generator/core.py
sed -i '' 's/, datetime//' src/forklift/schema/generator/core.py

# src/forklift/schema/generator/validation.py:7:1: F401 '..utils.helpers.SchemaValidationError' imported but unused
sed -i '' '/from \.\.utils\.helpers import SchemaValidationError/d' src/forklift/schema/generator/validation.py

# src/forklift/schema/processors/config_parser.py:3:1: F401 'typing.List' imported but unused
sed -i '' 's/from typing import List, /from typing import /' src/forklift/schema/processors/config_parser.py
sed -i '' 's/from typing import List$/from typing import/' src/forklift/schema/processors/config_parser.py
sed -i '' 's/, List//' src/forklift/schema/processors/config_parser.py

# src/forklift/schema/processors/metadata.py:4:1: F401 'typing.List' imported but unused
sed -i '' 's/from typing import List, /from typing import /' src/forklift/schema/processors/metadata.py
sed -i '' 's/from typing import List$/from typing import/' src/forklift/schema/processors/metadata.py
sed -i '' 's/, List//' src/forklift/schema/processors/metadata.py

# src/forklift/schema/types/special_types.py:6:1: F401 'pandas as pd' imported but unused
sed -i '' '/^import pandas as pd$/d' src/forklift/schema/types/special_types.py

# src/forklift/schema/types/transformations.py:4:1: F401 'typing.List' imported but unused
# src/forklift/schema/types/transformations.py:6:1: F401 'pandas as pd' imported but unused
sed -i '' 's/from typing import List, /from typing import /' src/forklift/schema/types/transformations.py
sed -i '' 's/from typing import List$/from typing import/' src/forklift/schema/types/transformations.py
sed -i '' 's/, List//' src/forklift/schema/types/transformations.py
sed -i '' '/^import pandas as pd$/d' src/forklift/schema/types/transformations.py

# src/forklift/schema/utils/formatters.py:5:1: F401 'typing.Optional' imported but unused
sed -i '' 's/from typing import Optional, /from typing import /' src/forklift/schema/utils/formatters.py
sed -i '' 's/from typing import Optional$/from typing import/' src/forklift/schema/utils/formatters.py
sed -i '' 's/, Optional//' src/forklift/schema/utils/formatters.py

# src/forklift/utils/transformations/__init__.py:19:1: F401 '.configs.*' imported but unused
sed -i '' '/from \.configs import \*/d' src/forklift/utils/transformations/__init__.py

# src/forklift/utils/transformations/base.py:9:1: F401 'decimal.Decimal' imported but unused
# src/forklift/utils/transformations/base.py:12:1: F401 'pandas as pd' imported but unused
sed -i '' 's/from decimal import Decimal, /from decimal import /' src/forklift/utils/transformations/base.py
sed -i '' 's/from decimal import Decimal$/from decimal import/' src/forklift/utils/transformations/base.py
sed -i '' 's/, Decimal//' src/forklift/utils/transformations/base.py
sed -i '' '/^import pandas as pd$/d' src/forklift/utils/transformations/base.py

# src/forklift/utils/transformations/configs.py:9:1: F401 'typing.Any' imported but unused
sed -i '' 's/from typing import Any, /from typing import /' src/forklift/utils/transformations/configs.py
sed -i '' 's/from typing import Any$/from typing import/' src/forklift/utils/transformations/configs.py
sed -i '' 's/, Any//' src/forklift/utils/transformations/configs.py

# Clean up any empty import lines that might remain
find src/ -name "*.py" -exec sed -i '' '/^from typing import$/d' {} \;
find src/ -name "*.py" -exec sed -i '' '/^from datetime import$/d' {} \;
find src/ -name "*.py" -exec sed -i '' '/^from decimal import$/d' {} \;

echo "✅ Finished fixing unused imports!"
echo "🔍 Running flake8 to check F401 errors..."

# Check if F401 errors are fixed
python -m flake8 src/ --max-line-length=99 --extend-ignore=E203,W503 | grep F401
if test $status -eq 0
    echo "⚠️  Some F401 errors still remain (see above)"
else
    echo "🎉 All F401 errors have been fixed!"
end
