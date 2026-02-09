const modalButtons = document.querySelectorAll("[data-modal-target]");
const closeButtons = document.querySelectorAll("[data-close]");
const scrollButtons = document.querySelectorAll("[data-scroll-target]");
const checkoutButtons = document.querySelectorAll("[data-checkout]");

modalButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const targetId = button.getAttribute("data-modal-target");
    const modal = document.getElementById(targetId);
    if (modal) {
      modal.classList.add("open");
    }
  });
});

closeButtons.forEach((button) => {
  button.addEventListener("click", () => {
    button.closest(".modal").classList.remove("open");
  });
});

scrollButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const target = document.querySelector(button.getAttribute("data-scroll-target"));
    if (target) {
      target.scrollIntoView({ behavior: "smooth" });
    }
  });
});

checkoutButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const tier = button.getAttribute("data-checkout");
    alert(
      `Stripe checkout placeholder for ${tier}. Replace this with your live Stripe checkout link.`
    );
  });
});

window.addEventListener("click", (event) => {
  if (event.target.classList.contains("modal")) {
    event.target.classList.remove("open");
  }
});
