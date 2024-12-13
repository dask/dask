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
  script.setAttribute("runllm-slack-community-url", "https://dask.slack.com/");
  script.setAttribute("runllm-per-user-usage-limit", 5);
  script.setAttribute("runllm-usage-limit-effective-days", 30);
  script.setAttribute("runllm-usage-limit-message", `Hi! You've hit the limit for questions here in the docs, but you can join us on the Dask community Slack and ask as many questions as you'd like in #DaskBot channel. Hope to see you there.`);
  script.setAttribute("runllm-brand-logo", "_images/dask_icon.svg");
  script.setAttribute("runllm-floating-button-text", "Ask DaskBot");
  script.setAttribute("runllm-join-community-text", "Chat with DaskBot in Slack (join the #DaskBot channel)");

  script.async = true;
  document.head.appendChild(script);
});
