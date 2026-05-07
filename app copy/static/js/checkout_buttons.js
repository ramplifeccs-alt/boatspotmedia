document.addEventListener("DOMContentLoaded", function(){
  document.querySelectorAll("[data-product-id]").forEach(function(card){
    if (!card.querySelector(".stripe-buy-product")) {
      const id = card.getAttribute("data-product-id");
      const a = document.createElement("a");
      a.className = "btn stripe-buy-product";
      a.href = "/checkout/product/" + id;
      a.textContent = "Buy Now";
      card.appendChild(a);
    }
  });
});