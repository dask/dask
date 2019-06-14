if (location.protocol == 'http:') {
 location.href = 'https:' + window.location.href.substring(window.location.protocol.length);
}