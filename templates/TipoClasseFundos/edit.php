<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoClasseFundo $tipoClasseFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $tipoClasseFundo->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $tipoClasseFundo->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Tipo Classe Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoClasseFundos form content">
            <?= $this->Form->create($tipoClasseFundo) ?>
            <fieldset>
                <legend><?= __('Edit Tipo Classe Fundo') ?></legend>
                <?php
                    echo $this->Form->control('classe');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
