<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\AdministradorFundo $administradorFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $administradorFundo->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $administradorFundo->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Administrador Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="administradorFundos form content">
            <?= $this->Form->create($administradorFundo) ?>
            <fieldset>
                <legend><?= __('Edit Administrador Fundo') ?></legend>
                <?php
                    echo $this->Form->control('cnpj');
                    echo $this->Form->control('nome');
                    echo $this->Form->control('DT_REG_CVM');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
