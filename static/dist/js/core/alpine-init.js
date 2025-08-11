// Не используем import вообще!

document.addEventListener('alpine:init', () => {
  Alpine.data('catalog', window.catalog);
  Alpine.data('sidebar', window.sidebar);
  Alpine.data('homeCategories', window.homeCategories);
  Alpine.data('product', window.product);
  Alpine.data('searchModal', window.searchModal);
});
