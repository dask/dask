document.addEventListener("DOMContentLoaded", function () {
  var script = document.createElement("script");
  script.type = "module";
  script.id = "runllm-widget-script"

  script.src = "https://widget.runllm.com";

  script.setAttribute("version", "stable");
  script.setAttribute("runllm-keyboard-shortcut", "Mod+j"); // cmd-j or ctrl-j to open the widget.
  script.setAttribute("runllm-name", "DaskBot");
  script.setAttribute("runllm-position", "BOTTOM_RIGHT"); // put above ethical ads
  script.setAttribute("runllm-position-x", "20px");
  script.setAttribute("runllm-position-y", "50%");
  script.setAttribute("runllm-assistant-id", "273");
  script.setAttribute("runllm-theme-color", "#FFC11E");
  script.setAttribute("runllm-slack-community-url", "TODO");
  script.setAttribute("runllm-per-user-usage-limit", 2);
  script.setAttribute("runllm-usage-limit-effective-days", 30);
  script.setAttribute("runllm-usage-limit-message", `#Hello!
You are out of queries.`);
  script.setAttribute("runllm-brand-logo", "_images/dask_icon.svg");

  script.async = true;
  document.head.appendChild(script);
});
