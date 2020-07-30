/**
 * On window load check for the "configConvertUtilYAML" and "configConvertUtilEnv" text areas.
 * If they exist add event handlers to convert YAML to environment variables and vice-versa
 * when the user types in either field.
 *
 */
window.addEventListener('load', (event) => {
    var configConvertUtilYAML = document.getElementById("configConvertUtilYAML")
    var configConvertUtilEnv = document.getElementById("configConvertUtilEnv")
    var configConvertUtilCode = document.getElementById("configConvertUtilCode")

    if (
        typeof (configConvertUtilYAML) != 'undefined' &&
        configConvertUtilYAML != null &&
        typeof (configConvertUtilEnv) != 'undefined' &&
        configConvertUtilEnv != null &&
        typeof (configConvertUtilCode) != 'undefined' &&
        configConvertUtilCode != null) {

        configConvertUtilYAML.addEventListener("keyup", (event) => {
            try {
                config = parseYAML(configConvertUtilYAML.value)
                configConvertUtilEnv.value = dumpEnv(config)
                configConvertUtilCode.value = dumpCode(config)
            }
            catch (err) {
                console.log(err)
                configConvertUtilEnv.value = err.message
                configConvertUtilCode.value = err.message
            }

        })
        configConvertUtilEnv.addEventListener("keyup", (event) => {
            config = parseEnv(configConvertUtilEnv.value)
            configConvertUtilYAML.value = dumpYAML(config)
            configConvertUtilCode.value = dumpCode(config)
        })
        configConvertUtilCode.addEventListener("keyup", (event) => {
            config = parseCode(configConvertUtilCode.value)
            configConvertUtilYAML.value = dumpYAML(config)
            configConvertUtilEnv.value = dumpEnv(config)
        })

    }
});

/**
 * Parse a YAML string into a JS object.
 *
 * @param {string} raw - Raw YAML input
 * @return {object} The parsed data
 */
function parseYAML(raw) {
    return jsyaml.load(raw)
}

/**
 * Parse an environment variable declaration into a JS object.
 *
 * @param {string} raw - Raw environment variable declaration
 * @return {object} The parsed data
 */
function parseEnv(raw) {
    objects = []
    raw = raw.trim()
    raw.split("\n").forEach(envVar => {
        let x
        x = envVar.split("DASK_")
        envVar = x[1]
        x = envVar.split("=")
        envVar = x[0]
        let namespace = envVar.split("__").map(toKey)
        let value = x[1]
        while (true) {
            if (value === true || value === false) {
                break
            }
            try {
                value = JSON.parse(value)
            } catch (err) {
                break
            }
        }
        let object = value
        for (var i = namespace.length - 1; i >= 0; i--) {
            new_object = {}
            new_object[namespace[i]] = object
            object = new_object
        }
        objects.push(object)
    })
    return mergeDeep(...objects)
}

function parseCode(raw) {
    objects = []
    raw = raw.trim()
    raw.split("\n").forEach(envVar => {
        let x
        x = envVar.split("set({\"")
        envVar = x[1]
        x = envVar.split("})")
        envVar = x[0]
        x = envVar.split("\": ")
        envVar = x[0]
        let namespace = envVar.split(".").map(toKey)
        let value = x[1]
        while (true) {
            if (value === "True") {
                value = true
            }
            if (value === "False") {
                value = false
            }
            if (value === true || value === false) {
                break
            }
            try {
                value = JSON.parse(value)
            } catch (err) {
                break
            }
        }
        let object = value
        for (var i = namespace.length - 1; i >= 0; i--) {
            new_object = {}
            new_object[namespace[i]] = object
            object = new_object
        }
        objects.push(object)
    })
    return mergeDeep(...objects)
}

/**
 * Dump a JS config object to a YAML string.
 *
 * @param {string} config - Config object
 * @return {str} YAML configuration
 */
function dumpYAML(config) {
    return jsyaml.dump(config)
}

/**
 * Dump a JS config object to a set of environment variable declarations.
 *
 * @param {string} config - Config object
 * @return {string} environment variable declarations
 *
 * @example
 * dumpEnv({"array": {"chunk-size": "128 MB"}})
 * // returns 'export DASK_ARRAY__CHUNK_SIZE="128 MB"'
 */
function dumpEnv(config) {
    return walkEnv(config).join("\n")
}

/**
 * Walk through config object and construct env var namespace. Recursive method.
 *
 * @param {string} config - Config object
 * @param {string} prefix - The prefix for the current level of nesting. Always starts with "DASK_"
 * @return {list} list of environment variable declarations
 *
 * @example
 * walkEnv({"array": {"chunk-size": "128 MB"}})
 * // returns ['export DASK_ARRAY__CHUNK_SIZE="128 MB"']
 */
function walkEnv(config, prefix = "DASK_") {
    let vars = []
    if (config === null) {
        return config
    }
    Object.keys(config).forEach(key => {
        if (typeof (config[key]) === 'object' && !Array.isArray(config[key])) {
            vars = vars.concat(walkEnv(config[key], `${prefix + fromKey(key)}__`))
        } else {
            vars.push(`export ${prefix}${fromKey(key)}=${JSON.stringify(config[key])}`)
        }
    });
    return vars
}

function dumpCode(config) {
    return walkCode(config).join("\n")
}

/**
 * Walk through config object and construct env var namespace. Recursive method.
 *
 * @param {string} config - Config object
 * @param {string} prefix - The prefix for the current level of nesting. Always starts with "DASK_"
 * @return {list} list of environment variable declarations
 *
 * @example
 * walkEnv({"array": {"chunk-size": "128 MB"}})
 * // returns ['export DASK_ARRAY__CHUNK_SIZE="128 MB"']
 */
function walkCode(config, prefix = "") {
    let vars = []
    if (config === null) {
        return config
    }
    Object.keys(config).forEach(key => {
        if (typeof (config[key]) === 'object' && !Array.isArray(config[key])) {
            vars = vars.concat(walkCode(config[key], `${prefix + key}.`))
        } else {
            value = JSON.stringify(config[key])
            if (value === true || value === "true") {
                value = "True"
            }
            if (value === false || value === "false") {
                value = "False"
            }
            vars.push(`>>> dask.config.set({"${prefix}${key}": ${value}})`)
        }
    });
    return vars
}

/**
 * Convert environment variable style key to YAML style key
 *
 * @param {string} str - Environment variable style key
 * @return {string} YAML style key
 *
 * @example
 * toKey("HELLO_WORLD")
 * // returns "hello-world"
 */
function toKey(str) {
    return str.toLowerCase().replace("_", "-")
}

/**
 * Convert YAML style key to environment variable style key
 *
 * @param {string} str - YAML style key
 * @return {string} Environment variable style key
 *
 * @example
 * toKey("hello-world")
 * // returns "HELLO_WORLD"
 */
function fromKey(key) {
    return key.toUpperCase().replace("-", "_")
}

/**
 * Deep merge a set of objects
 *
 * @param {list} objects - Objects to be merged (wrapped arguments)
 * @return {object} Merged object
 *
 * @example
 * mergeDeep({"hello": "world"}, {"foo": "bar"})
 * // returns {"hello": "world", "foo": "bar"}
 */
function mergeDeep(...objects) {
    const isObject = obj => obj && typeof obj === 'object';

    return objects.reduce((prev, obj) => {
        Object.keys(obj).forEach(key => {
            const pVal = prev[key];
            const oVal = obj[key];

            if (Array.isArray(pVal) && Array.isArray(oVal)) {
                prev[key] = pVal.concat(...oVal);
            }
            else if (isObject(pVal) && isObject(oVal)) {
                prev[key] = mergeDeep(pVal, oVal);
            }
            else {
                prev[key] = oVal;
            }
        });

        return prev;
    }, {});
}